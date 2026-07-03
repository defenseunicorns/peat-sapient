//! `SapientBridge` — HLDMM-mode TCP server with DIL outbound task queue.
//!
//! The bridge routes inbound SAPIENT messages to `SapientUpdate` events and
//! delivers outbound `Task` messages to connected DLMMs, queuing them for
//! replay on reconnect.

use std::collections::HashMap;
use std::sync::Arc;

use futures_util::{SinkExt, StreamExt};
use peat_schema::{
    capability::v1::CapabilityAdvertisement,
    node::v1::NodeState,
    track::v1::{Track, TrackPosition},
};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, Mutex};
use tokio_util::codec::{FramedRead, FramedWrite};
use tracing::{debug, info, warn};

use crate::{
    codec::SapientCodec,
    error::SapientError,
    proto::sapient_msg::bsi_flex_335_v2_0::{
        sapient_message::Content, task_ack, AlertAck as ProtoAlertAck, RegistrationAck,
        SapientMessage,
    },
    rate_limit::{DetectionLimiter, RateLimitConfig},
    registry::{get_position, new_registry, remove, upsert, NodeRegistry},
    task_queue::TaskQueue,
    transform::{alert, alert::SapientAlertEvent, detection, registration, status},
};

/// Status reported by a DLMM in a `TaskAck`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskAckStatus {
    Accepted,
    Rejected,
    Completed,
    Failed,
}

impl TaskAckStatus {
    fn from_proto(status: Option<i32>) -> Self {
        status
            .and_then(|s| task_ack::TaskStatus::try_from(s).ok())
            .map(|s| match s {
                task_ack::TaskStatus::Accepted => Self::Accepted,
                task_ack::TaskStatus::Rejected => Self::Rejected,
                task_ack::TaskStatus::Completed => Self::Completed,
                task_ack::TaskStatus::Failed => Self::Failed,
                task_ack::TaskStatus::Unspecified => Self::Failed,
            })
            .unwrap_or(Self::Failed)
    }
}

/// Status reported in an `AlertAck`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AlertAckStatus {
    Accepted,
    Rejected,
    Cancelled,
}

impl AlertAckStatus {
    fn from_proto(status: Option<i32>) -> Self {
        use crate::proto::sapient_msg::bsi_flex_335_v2_0::alert_ack;
        status
            .and_then(|s| alert_ack::AlertAckStatus::try_from(s).ok())
            .map(|s| match s {
                alert_ack::AlertAckStatus::Accepted => Self::Accepted,
                alert_ack::AlertAckStatus::Rejected => Self::Rejected,
                alert_ack::AlertAckStatus::Cancelled => Self::Cancelled,
                alert_ack::AlertAckStatus::Unspecified => Self::Rejected,
            })
            .unwrap_or(Self::Rejected)
    }

    fn to_proto(self) -> i32 {
        use crate::proto::sapient_msg::bsi_flex_335_v2_0::alert_ack;
        match self {
            Self::Accepted => alert_ack::AlertAckStatus::Accepted as i32,
            Self::Rejected => alert_ack::AlertAckStatus::Rejected as i32,
            Self::Cancelled => alert_ack::AlertAckStatus::Cancelled as i32,
        }
    }
}

/// A bridge update produced by routing one inbound SAPIENT message.
#[derive(Debug)]
pub enum SapientUpdate {
    /// A new DLMM sensor connected and sent its `Registration`.
    Registered {
        node_id: String,
        advertisement: CapabilityAdvertisement,
    },
    /// A DLMM sent a `StatusReport`.
    StatusUpdated {
        node_id: String,
        state: NodeState,
        /// Present when the report carries FOV / mode data.
        capability_delta: Option<CapabilityAdvertisement>,
    },
    /// A DLMM sent a `DetectionReport` that has been mapped to a peat `Track`.
    Detected { node_id: String, track: Track },
    /// A DLMM sent an `Alert`.
    Alerted {
        node_id: String,
        event: SapientAlertEvent,
    },
    /// A node acknowledged a previously-sent alert.
    AlertAcknowledged {
        node_id: String,
        alert_id: String,
        status: AlertAckStatus,
        reasons: Vec<String>,
    },
    /// A DLMM acknowledged a task sent by the bridge.
    TaskAcknowledged {
        node_id: String,
        task_id: String,
        /// The peat `command_id` that produced this task, if the task was sent
        /// via `send_task` with a `command_id`. Consumers use this to construct
        /// a `CommandAcknowledgment` for the originating hierarchy level.
        command_id: Option<String>,
        status: TaskAckStatus,
        reasons: Vec<String>,
    },
    /// Message was received but has no peat mapping (e.g. `RegistrationAck`, `Error`).
    Ignored { reason: String },
    /// A node stopped sending heartbeats (watchdog timeout) or closed its TCP
    /// connection. The node has already been removed from the `NodeRegistry`.
    NodeDisconnected { node_id: String },
}

/// Bridge configuration.
#[derive(Debug, Clone)]
pub struct BridgeConfig {
    /// UUID this bridge presents as its SAPIENT `node_id`.
    pub node_id: String,
    /// TCP address to bind (`start()` listens here in HLDMM mode).
    pub addr: std::net::SocketAddr,
    /// Per-node `DetectionReport` rate limit. `None` disables rate limiting.
    pub detection_rate_limit: Option<RateLimitConfig>,
    /// Interval between heartbeat checks. Nodes silent for `2 ×` this duration
    /// emit `NodeDisconnected`. Defaults to 30 s per the SAPIENT ICD.
    pub heartbeat_interval: std::time::Duration,
    /// Maximum number of unacknowledged outbound tasks queued per DLMM node.
    /// When full, the oldest pending task is evicted. Recommended value: 32.
    pub task_queue_depth: usize,
    /// Maximum age of a queued task before it is discarded rather than replayed
    /// on reconnect. Recommended value: 300 s (5 min).
    pub task_ttl: std::time::Duration,
}

/// Shared runtime state held behind an `Arc` so the accept-loop background
/// tasks and the public `send_task()` / `registry()` API share it safely.
struct BridgeInner {
    config: BridgeConfig,
    detection_limiter: Option<DetectionLimiter>,
    /// `node_id` → sender for writing to that DLMM's live TCP connection.
    connections: Mutex<HashMap<String, mpsc::Sender<SapientMessage>>>,
    /// Per-node DIL outbound task queue.
    task_queue: Mutex<TaskQueue>,
    /// `task_id` → originating peat `command_id`, for correlating `TaskAck`
    /// back to the `HierarchicalCommand` that produced the task.
    task_commands: Mutex<HashMap<String, String>>,
    /// Registry of all registered (connected) SAPIENT nodes.
    registry: NodeRegistry,
    /// Emits `SapientUpdate` events to the application receive loop.
    update_tx: mpsc::Sender<SapientUpdate>,
}

/// SAPIENT bridge: HLDMM-mode TCP server that routes inbound sensor messages
/// and queues outbound `Task` messages with DIL replay on reconnect.
pub struct SapientBridge {
    inner: Arc<BridgeInner>,
}

impl SapientBridge {
    /// Create a new bridge. Returns `(bridge, update_receiver)`.
    ///
    /// The application drives `update_receiver` to consume `SapientUpdate`
    /// events from the bridge. Call `start()` to begin accepting connections.
    pub fn new(config: BridgeConfig) -> (Self, mpsc::Receiver<SapientUpdate>) {
        let (update_tx, update_rx) = mpsc::channel(256);
        let detection_limiter = config.detection_rate_limit.map(DetectionLimiter::new);
        let task_queue = Mutex::new(TaskQueue::new(config.task_queue_depth, config.task_ttl));
        let inner = Arc::new(BridgeInner {
            detection_limiter,
            task_queue,
            task_commands: Mutex::new(HashMap::new()),
            connections: Mutex::new(HashMap::new()),
            registry: new_registry(),
            update_tx,
            config,
        });
        (Self { inner }, update_rx)
    }

    /// Start the bridge in HLDMM mode: bind `config.addr` and accept incoming
    /// DLMM connections. Each connection is handled in a dedicated tokio task.
    ///
    /// Returns the actual bound address after spawning the accept loop. When
    /// `config.addr` uses port `0`, the OS assigns an ephemeral port — use
    /// the returned address to connect. Returns `Err` if the TCP bind fails.
    pub async fn start(&self) -> Result<std::net::SocketAddr, SapientError> {
        let listener = TcpListener::bind(self.inner.config.addr)
            .await
            .map_err(|e| {
                SapientError::ConnectionFailed(format!("bind {}: {e}", self.inner.config.addr))
            })?;
        let bound_addr = listener
            .local_addr()
            .map_err(|e| SapientError::ConnectionFailed(format!("local_addr: {e}")))?;
        info!(addr = %bound_addr, "SAPIENT HLDMM listening");

        let inner = Arc::clone(&self.inner);
        tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((stream, peer)) => {
                        debug!(%peer, "SAPIENT DLMM connected");
                        let inner_c = Arc::clone(&inner);
                        tokio::spawn(run_connection(stream, inner_c));
                    }
                    Err(e) => warn!(error = %e, "SAPIENT accept error"),
                }
            }
        });

        Ok(bound_addr)
    }

    /// Enqueue a `SapientMessage` carrying a `Task` for delivery to `node_id`.
    ///
    /// The task is enqueued immediately. If the DLMM is currently connected, it
    /// is also sent without waiting for the queue. The task remains queued until
    /// a `TaskAck` is received; if the DLMM disconnects before acknowledging,
    /// the task is replayed on the next reconnect (unless its TTL has elapsed).
    ///
    /// `command_id` — the peat `HierarchicalCommand.command_id` that produced
    /// this task. When the DLMM sends a `TaskAck`, the resulting
    /// `TaskAcknowledged` update will carry this value so consumers can
    /// construct a `CommandAcknowledgment` for the originating hierarchy level.
    /// Pass `None` when the task doesn't originate from a peat command.
    ///
    /// Returns `Err` if the message does not carry a `Task` with a non-empty
    /// `task_id`.
    pub async fn send_task(
        &self,
        node_id: &str,
        msg: SapientMessage,
        command_id: Option<String>,
    ) -> Result<(), SapientError> {
        let task_id = extract_task_id(&msg)?;

        if let Some(ref cmd_id) = command_id {
            self.inner
                .task_commands
                .lock()
                .await
                .insert(task_id.clone(), cmd_id.clone());
        }

        {
            let mut q = self.inner.task_queue.lock().await;
            q.drain_expired();
            q.enqueue(node_id, task_id, msg.clone());
        }

        // If the node is connected, send immediately. Errors are intentionally
        // ignored — the task is in the queue and will be replayed on reconnect.
        let conns = self.inner.connections.lock().await;
        if let Some(tx) = conns.get(node_id) {
            tx.send(msg).await.ok();
        }

        Ok(())
    }

    /// Send an `AlertAck` to a connected DLMM.
    ///
    /// Unlike `send_task`, alert acknowledgements are not queued for DIL
    /// replay. Returns `Err(NodeNotFound)` if the node is not connected.
    pub async fn send_alert_ack(
        &self,
        node_id: &str,
        alert_id: &str,
        status: AlertAckStatus,
        reasons: Vec<String>,
    ) -> Result<(), SapientError> {
        let msg = SapientMessage {
            node_id: Some(self.inner.config.node_id.clone()),
            destination_id: Some(node_id.to_string()),
            timestamp: Some(now_proto_ts()),
            content: Some(Content::AlertAck(ProtoAlertAck {
                alert_id: Some(alert_id.to_string()),
                reason: reasons,
                alert_ack_status: Some(status.to_proto()),
            })),
            additional_information: None,
        };

        let conns = self.inner.connections.lock().await;
        if let Some(tx) = conns.get(node_id) {
            tx.send(msg).await.ok();
            Ok(())
        } else {
            Err(SapientError::NodeNotFound(node_id.to_string()))
        }
    }

    /// Return a clone of the `Arc` wrapping the node registry.
    ///
    /// Use this to pass the registry to `run_watchdog`:
    /// ```rust,no_run
    /// # use peat_sapient::bridge::{BridgeConfig, SapientBridge};
    /// # use peat_sapient::watchdog::run_watchdog;
    /// # use tokio::sync::mpsc;
    /// # use std::time::Duration;
    /// # async fn example() {
    /// # let config = BridgeConfig {
    /// #     node_id: "id".into(), addr: "0.0.0.0:5066".parse().unwrap(),
    /// #     detection_rate_limit: None, heartbeat_interval: Duration::from_secs(30),
    /// #     task_queue_depth: 32, task_ttl: Duration::from_secs(300),
    /// # };
    /// let (bridge, mut updates) = SapientBridge::new(config);
    /// bridge.start().await.unwrap();
    /// let (wd_tx, mut wd_rx) = mpsc::channel(64);
    /// tokio::spawn(run_watchdog(bridge.registry(), Duration::from_secs(30), wd_tx));
    /// # }
    /// ```
    pub fn registry(&self) -> NodeRegistry {
        Arc::clone(&self.inner.registry)
    }
}

/// Run the per-connection handler loop for one accepted DLMM TCP connection.
async fn run_connection(stream: TcpStream, inner: Arc<BridgeInner>) {
    let (read_half, write_half) = tokio::io::split(stream);
    let mut framed_read = FramedRead::new(read_half, SapientCodec);
    let mut framed_write = FramedWrite::new(write_half, SapientCodec);

    // Per-connection channel: callers write here; write task drains to the socket.
    let (write_tx, mut write_rx) = mpsc::channel::<SapientMessage>(64);

    tokio::spawn(async move {
        while let Some(msg) = write_rx.recv().await {
            if let Err(e) = framed_write.send(msg).await {
                warn!(error = %e, "SAPIENT write error — closing write half");
                break;
            }
        }
    });

    let mut registered_node_id: Option<String> = None;

    while let Some(result) = framed_read.next().await {
        let msg = match result {
            Ok(m) => m,
            Err(e) => {
                warn!(error = %e, "SAPIENT read error");
                break;
            }
        };

        let msg_node_id = msg.node_id.clone().unwrap_or_default();

        // On first Registration from this connection: register, send RegistrationAck,
        // and replay any pending DIL tasks for this node.
        if matches!(&msg.content, Some(Content::Registration(_))) && registered_node_id.is_none() {
            registered_node_id = Some(msg_node_id.clone());
            inner
                .connections
                .lock()
                .await
                .insert(msg_node_id.clone(), write_tx.clone());

            write_tx
                .send(make_registration_ack(&inner.config.node_id, &msg_node_id))
                .await
                .ok();

            let pending = {
                let mut q = inner.task_queue.lock().await;
                q.drain_expired();
                q.pending_for(&msg_node_id)
            };
            for task_msg in pending {
                write_tx.send(task_msg).await.ok();
            }
        }

        // Route the message and emit a SapientUpdate.
        let sensor_pos = get_position(&inner.registry, &msg_node_id).await;
        match route_message(msg, sensor_pos.as_ref(), inner.detection_limiter.as_ref()) {
            Ok(mut update) => {
                match &update {
                    SapientUpdate::Registered {
                        node_id,
                        advertisement,
                    } => {
                        upsert(&inner.registry, node_id, Some(advertisement.clone()), None).await;
                    }
                    SapientUpdate::StatusUpdated {
                        node_id,
                        capability_delta,
                        ..
                    } => {
                        upsert(&inner.registry, node_id, capability_delta.clone(), None).await;
                    }
                    SapientUpdate::Detected { node_id, .. } => {
                        upsert(&inner.registry, node_id, None, None).await;
                    }
                    SapientUpdate::TaskAcknowledged {
                        ref node_id,
                        ref task_id,
                        ref status,
                        ..
                    } => {
                        inner.task_queue.lock().await.ack(node_id, task_id);
                        if *status == TaskAckStatus::Failed {
                            warn!(
                                node_id = %node_id,
                                task_id = %task_id,
                                "DLMM reported task failed"
                            );
                        }
                    }
                    _ => {}
                }
                // Enrich TaskAcknowledged with the originating command_id.
                if let SapientUpdate::TaskAcknowledged {
                    ref task_id,
                    ref mut command_id,
                    ..
                } = update
                {
                    *command_id = inner.task_commands.lock().await.remove(task_id);
                }
                inner.update_tx.send(update).await.ok();
            }
            Err(e) => {
                warn!(error = %e, node_id = %msg_node_id, "route_message error");
            }
        }
    }

    // Clean up on disconnect.
    if let Some(nid) = registered_node_id {
        inner.connections.lock().await.remove(&nid);
        remove(&inner.registry, &nid).await;
        inner
            .update_tx
            .send(SapientUpdate::NodeDisconnected { node_id: nid })
            .await
            .ok();
    }
}

fn make_registration_ack(hldmm_node_id: &str, destination_id: &str) -> SapientMessage {
    SapientMessage {
        node_id: Some(hldmm_node_id.to_string()),
        destination_id: Some(destination_id.to_string()),
        timestamp: Some(now_proto_ts()),
        content: Some(Content::RegistrationAck(RegistrationAck {
            acceptance: Some(true),
            ack_response_reason: vec![],
        })),
        additional_information: None,
    }
}

fn now_proto_ts() -> prost_types::Timestamp {
    let d = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    prost_types::Timestamp {
        seconds: d.as_secs() as i64,
        nanos: d.subsec_nanos() as i32,
    }
}

/// Extract `task_id` from a `SapientMessage` wrapping a `Task`.
///
/// Returns `Err` if the message does not carry `Content::Task` or if `task_id`
/// is empty.
fn extract_task_id(msg: &SapientMessage) -> Result<String, SapientError> {
    match &msg.content {
        Some(Content::Task(task)) => {
            let id = task.task_id.clone().unwrap_or_default();
            if id.is_empty() {
                Err(SapientError::MappingError {
                    kind: "task_id",
                    detail: "Task message has no task_id".into(),
                })
            } else {
                Ok(id)
            }
        }
        _ => Err(SapientError::MappingError {
            kind: "content",
            detail: "send_task requires a SapientMessage with Task content".into(),
        }),
    }
}

/// Route a single inbound SAPIENT message to a `SapientUpdate`.
///
/// `sensor_position` — if `Some`, used to resolve range-bearing detections.
/// Passing `None` causes `UnsupportedCoordinateSystem` for range-bearing reports.
///
/// `detection_limiter` — if `Some`, `DetectionReport` messages that exceed the
/// per-node token bucket are dropped as `Ignored` rather than emitted as
/// `Detected`.
///
/// All unhandled `Content` variants (e.g. `RegistrationAck`, `AlertAck`,
/// `Error`) produce `SapientUpdate::Ignored` so unexpected messages never panic
/// the bridge loop.
pub fn route_message(
    msg: SapientMessage,
    sensor_position: Option<&TrackPosition>,
    detection_limiter: Option<&DetectionLimiter>,
) -> Result<SapientUpdate, SapientError> {
    let node_id = msg.node_id.clone().unwrap_or_default();

    match msg.content {
        Some(Content::Registration(reg)) => {
            let advertisement = registration::from_registration(&node_id, &reg);
            Ok(SapientUpdate::Registered {
                node_id,
                advertisement,
            })
        }

        Some(Content::StatusReport(sr)) => {
            let (state, capability_delta) = status::from_status_report(&node_id, &sr);
            Ok(SapientUpdate::StatusUpdated {
                node_id,
                state,
                capability_delta,
            })
        }

        Some(Content::DetectionReport(dr)) => {
            if let Some(limiter) = detection_limiter {
                if !limiter.check(&node_id) {
                    let reason = format!("detection rate-limited for node {node_id}");
                    warn!(node_id = %node_id, "DetectionReport rate-limited — dropping");
                    return Ok(SapientUpdate::Ignored { reason });
                }
            }
            let track = detection::from_detection_report(&node_id, sensor_position, &dr)?;
            Ok(SapientUpdate::Detected { node_id, track })
        }

        Some(Content::Alert(a)) => {
            let event = alert::from_alert(&node_id, &a);
            Ok(SapientUpdate::Alerted { node_id, event })
        }

        Some(Content::TaskAck(ack)) => {
            let task_id = ack.task_id.unwrap_or_default();
            let status = TaskAckStatus::from_proto(ack.task_status);
            Ok(SapientUpdate::TaskAcknowledged {
                node_id,
                task_id,
                command_id: None,
                status,
                reasons: ack.reason,
            })
        }

        Some(Content::AlertAck(ack)) => {
            let alert_id = ack.alert_id.unwrap_or_default();
            let status = AlertAckStatus::from_proto(ack.alert_ack_status);
            Ok(SapientUpdate::AlertAcknowledged {
                node_id,
                alert_id,
                status,
                reasons: ack.reason,
            })
        }

        Some(other) => {
            let reason = format!("no peat mapping for {}", content_label(&other));
            warn!(node_id = %node_id, "{reason}");
            Ok(SapientUpdate::Ignored { reason })
        }

        None => {
            let reason = "SapientMessage has no content".to_string();
            warn!(node_id = %node_id, "{reason}");
            Ok(SapientUpdate::Ignored { reason })
        }
    }
}

fn content_label(c: &Content) -> &'static str {
    match c {
        Content::Registration(_) => "Registration",
        Content::RegistrationAck(_) => "RegistrationAck",
        Content::StatusReport(_) => "StatusReport",
        Content::DetectionReport(_) => "DetectionReport",
        Content::Task(_) => "Task",
        Content::TaskAck(_) => "TaskAck",
        Content::Alert(_) => "Alert",
        Content::AlertAck(_) => "AlertAck",
        Content::Error(_) => "Error",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::sapient_msg::bsi_flex_335_v2_0::{
        registration::{NodeDefinition, NodeType},
        status_report::System,
        Registration, SapientMessage, StatusReport,
    };

    fn msg_with(node_id: &str, content: Content) -> SapientMessage {
        SapientMessage {
            node_id: Some(node_id.to_string()),
            content: Some(content),
            ..Default::default()
        }
    }

    // --- Registration ---

    #[test]
    fn registration_routes_to_registered() {
        let reg = Registration {
            node_definition: vec![NodeDefinition {
                node_type: Some(NodeType::Camera as i32),
                node_sub_type: vec![],
            }],
            ..Default::default()
        };
        let update =
            route_message(msg_with("node-1", Content::Registration(reg)), None, None).unwrap();
        assert!(matches!(update, SapientUpdate::Registered { .. }));
    }

    #[test]
    fn registered_carries_correct_node_id() {
        let reg = Registration {
            node_definition: vec![NodeDefinition {
                node_type: Some(NodeType::Radar as i32),
                node_sub_type: vec![],
            }],
            ..Default::default()
        };
        let update = route_message(
            msg_with("sensor-uuid", Content::Registration(reg)),
            None,
            None,
        )
        .unwrap();
        if let SapientUpdate::Registered { node_id, .. } = update {
            assert_eq!(node_id, "sensor-uuid");
        } else {
            panic!("expected Registered");
        }
    }

    // --- StatusReport ---

    #[test]
    fn status_report_routes_to_status_updated() {
        let sr = StatusReport {
            system: Some(System::Ok as i32),
            ..Default::default()
        };
        let update =
            route_message(msg_with("node-2", Content::StatusReport(sr)), None, None).unwrap();
        assert!(matches!(update, SapientUpdate::StatusUpdated { .. }));
    }

    #[test]
    fn status_updated_node_id_matches() {
        let sr = StatusReport {
            system: Some(System::Warning as i32),
            ..Default::default()
        };
        let update =
            route_message(msg_with("my-sensor", Content::StatusReport(sr)), None, None).unwrap();
        if let SapientUpdate::StatusUpdated { node_id, .. } = update {
            assert_eq!(node_id, "my-sensor");
        } else {
            panic!("expected StatusUpdated");
        }
    }

    // --- DetectionReport ---

    #[test]
    fn detection_report_latlng_routes_to_detected() {
        use crate::proto::sapient_msg::bsi_flex_335_v2_0::{
            detection_report::LocationOneof, DetectionReport, Location, LocationCoordinateSystem,
            LocationDatum,
        };
        let dr = DetectionReport {
            report_id: Some("rpt-1".into()),
            object_id: Some("obj-1".into()),
            location_oneof: Some(LocationOneof::Location(Location {
                x: Some(-0.1278),
                y: Some(51.5074),
                z: Some(0.0),
                coordinate_system: Some(LocationCoordinateSystem::LatLngDegM as i32),
                datum: Some(LocationDatum::Wgs84E as i32),
                ..Default::default()
            })),
            ..Default::default()
        };
        let update = route_message(
            msg_with("sensor-1", Content::DetectionReport(dr)),
            None,
            None,
        )
        .unwrap();
        assert!(matches!(update, SapientUpdate::Detected { .. }));
    }

    // --- TaskAck ---

    #[test]
    fn task_ack_accepted_routes_to_task_acknowledged() {
        use crate::proto::sapient_msg::bsi_flex_335_v2_0::TaskAck;
        let ack = TaskAck {
            task_id: Some("01HZTASKID000000000000000000".into()),
            task_status: Some(task_ack::TaskStatus::Accepted as i32),
            ..Default::default()
        };
        let update = route_message(msg_with("n", Content::TaskAck(ack)), None, None).unwrap();
        if let SapientUpdate::TaskAcknowledged {
            task_id, status, ..
        } = update
        {
            assert_eq!(task_id, "01HZTASKID000000000000000000");
            assert_eq!(status, TaskAckStatus::Accepted);
        } else {
            panic!("expected TaskAcknowledged");
        }
    }

    #[test]
    fn task_ack_rejected_produces_rejected_status() {
        use crate::proto::sapient_msg::bsi_flex_335_v2_0::TaskAck;
        let ack = TaskAck {
            task_id: Some("task-rej".into()),
            task_status: Some(task_ack::TaskStatus::Rejected as i32),
            reason: vec!["out of fuel".into()],
            ..Default::default()
        };
        let update = route_message(msg_with("n", Content::TaskAck(ack)), None, None).unwrap();
        if let SapientUpdate::TaskAcknowledged {
            status, reasons, ..
        } = update
        {
            assert_eq!(status, TaskAckStatus::Rejected);
            assert_eq!(reasons, ["out of fuel"]);
        } else {
            panic!("expected TaskAcknowledged");
        }
    }

    #[test]
    fn task_ack_completed_produces_completed_status() {
        use crate::proto::sapient_msg::bsi_flex_335_v2_0::TaskAck;
        let ack = TaskAck {
            task_id: Some("task-done".into()),
            task_status: Some(task_ack::TaskStatus::Completed as i32),
            ..Default::default()
        };
        let update = route_message(msg_with("n", Content::TaskAck(ack)), None, None).unwrap();
        if let SapientUpdate::TaskAcknowledged { status, .. } = update {
            assert_eq!(status, TaskAckStatus::Completed);
        } else {
            panic!("expected TaskAcknowledged");
        }
    }

    #[test]
    fn task_ack_failed_produces_failed_status() {
        use crate::proto::sapient_msg::bsi_flex_335_v2_0::TaskAck;
        let ack = TaskAck {
            task_id: Some("task-fail".into()),
            task_status: Some(task_ack::TaskStatus::Failed as i32),
            reason: vec!["sensor malfunction".into()],
            ..Default::default()
        };
        let update = route_message(msg_with("n", Content::TaskAck(ack)), None, None).unwrap();
        if let SapientUpdate::TaskAcknowledged {
            status, reasons, ..
        } = update
        {
            assert_eq!(status, TaskAckStatus::Failed);
            assert_eq!(reasons, ["sensor malfunction"]);
        } else {
            panic!("expected TaskAcknowledged");
        }
    }

    #[test]
    fn task_ack_unspecified_defaults_to_failed() {
        use crate::proto::sapient_msg::bsi_flex_335_v2_0::TaskAck;
        let ack = TaskAck {
            task_id: Some("task-unk".into()),
            task_status: None,
            ..Default::default()
        };
        let update = route_message(msg_with("n", Content::TaskAck(ack)), None, None).unwrap();
        if let SapientUpdate::TaskAcknowledged { status, .. } = update {
            assert_eq!(status, TaskAckStatus::Failed);
        } else {
            panic!("expected TaskAcknowledged");
        }
    }

    // --- AlertAck ---

    #[test]
    fn alert_ack_accepted_routes_to_alert_acknowledged() {
        use crate::proto::sapient_msg::bsi_flex_335_v2_0::alert_ack;
        use crate::proto::sapient_msg::bsi_flex_335_v2_0::AlertAck;
        let ack = AlertAck {
            alert_id: Some("alert-001".into()),
            alert_ack_status: Some(alert_ack::AlertAckStatus::Accepted as i32),
            ..Default::default()
        };
        let update = route_message(msg_with("n", Content::AlertAck(ack)), None, None).unwrap();
        if let SapientUpdate::AlertAcknowledged {
            alert_id, status, ..
        } = update
        {
            assert_eq!(alert_id, "alert-001");
            assert_eq!(status, AlertAckStatus::Accepted);
        } else {
            panic!("expected AlertAcknowledged");
        }
    }

    #[test]
    fn alert_ack_rejected_produces_rejected_status() {
        use crate::proto::sapient_msg::bsi_flex_335_v2_0::alert_ack;
        use crate::proto::sapient_msg::bsi_flex_335_v2_0::AlertAck;
        let ack = AlertAck {
            alert_id: Some("alert-002".into()),
            alert_ack_status: Some(alert_ack::AlertAckStatus::Rejected as i32),
            reason: vec!["false positive".into()],
        };
        let update = route_message(msg_with("n", Content::AlertAck(ack)), None, None).unwrap();
        if let SapientUpdate::AlertAcknowledged {
            status, reasons, ..
        } = update
        {
            assert_eq!(status, AlertAckStatus::Rejected);
            assert_eq!(reasons, ["false positive"]);
        } else {
            panic!("expected AlertAcknowledged");
        }
    }

    #[test]
    fn alert_ack_cancelled_produces_cancelled_status() {
        use crate::proto::sapient_msg::bsi_flex_335_v2_0::alert_ack;
        use crate::proto::sapient_msg::bsi_flex_335_v2_0::AlertAck;
        let ack = AlertAck {
            alert_id: Some("alert-003".into()),
            alert_ack_status: Some(alert_ack::AlertAckStatus::Cancelled as i32),
            ..Default::default()
        };
        let update = route_message(msg_with("n", Content::AlertAck(ack)), None, None).unwrap();
        if let SapientUpdate::AlertAcknowledged { status, .. } = update {
            assert_eq!(status, AlertAckStatus::Cancelled);
        } else {
            panic!("expected AlertAcknowledged");
        }
    }

    // --- Ignored variants ---

    #[test]
    fn registration_ack_routes_to_ignored() {
        use crate::proto::sapient_msg::bsi_flex_335_v2_0::RegistrationAck;
        let update = route_message(
            msg_with("n", Content::RegistrationAck(RegistrationAck::default())),
            None,
            None,
        )
        .unwrap();
        assert!(matches!(update, SapientUpdate::Ignored { .. }));
    }

    #[test]
    fn no_content_routes_to_ignored() {
        let msg = SapientMessage {
            node_id: Some("n".into()),
            content: None,
            ..Default::default()
        };
        let update = route_message(msg, None, None).unwrap();
        assert!(matches!(update, SapientUpdate::Ignored { .. }));
    }

    #[test]
    fn error_content_routes_to_ignored() {
        use crate::proto::sapient_msg::bsi_flex_335_v2_0::Error as SapientProtoError;
        let update = route_message(
            msg_with(
                "n",
                Content::Error(SapientProtoError {
                    packet: None,
                    error_message: vec![],
                }),
            ),
            None,
            None,
        )
        .unwrap();
        assert!(matches!(update, SapientUpdate::Ignored { .. }));
    }

    // --- Alert ---

    #[test]
    fn alert_routes_to_alerted() {
        use crate::proto::sapient_msg::bsi_flex_335_v2_0::{
            alert::{AlertStatus, AlertType, DiscretePriority},
            Alert,
        };
        let a = Alert {
            alert_id: Some("01HZALERTTEST00000000000000".into()),
            alert_type: Some(AlertType::Warning as i32),
            status: Some(AlertStatus::Active as i32),
            priority: Some(DiscretePriority::High as i32),
            description: Some("test alert".into()),
            ..Default::default()
        };
        let update = route_message(msg_with("sensor-a", Content::Alert(a)), None, None).unwrap();
        assert!(matches!(update, SapientUpdate::Alerted { .. }));
    }

    #[test]
    fn alerted_carries_node_id_and_alert_id() {
        use crate::proto::sapient_msg::bsi_flex_335_v2_0::{alert::AlertType, Alert};
        let a = Alert {
            alert_id: Some("alert-uuid-001".into()),
            alert_type: Some(AlertType::Critical as i32),
            ..Default::default()
        };
        let update = route_message(msg_with("my-sensor", Content::Alert(a)), None, None).unwrap();
        if let SapientUpdate::Alerted { node_id, event } = update {
            assert_eq!(node_id, "my-sensor");
            assert_eq!(event.alert_id, "alert-uuid-001");
        } else {
            panic!("expected Alerted");
        }
    }

    // --- Rate limiting ---

    fn detection_msg(node_id: &str) -> SapientMessage {
        use crate::proto::sapient_msg::bsi_flex_335_v2_0::{
            detection_report::LocationOneof, DetectionReport, Location, LocationCoordinateSystem,
            LocationDatum,
        };
        msg_with(
            node_id,
            Content::DetectionReport(DetectionReport {
                report_id: Some("rpt-rl".into()),
                object_id: Some("obj-rl".into()),
                location_oneof: Some(LocationOneof::Location(Location {
                    x: Some(-0.1278),
                    y: Some(51.5074),
                    z: Some(0.0),
                    coordinate_system: Some(LocationCoordinateSystem::LatLngDegM as i32),
                    datum: Some(LocationDatum::Wgs84E as i32),
                    ..Default::default()
                })),
                ..Default::default()
            }),
        )
    }

    #[tokio::test(start_paused = true)]
    async fn burst_of_detections_drops_excess() {
        use crate::rate_limit::{DetectionLimiter, RateLimitConfig};
        let limiter = DetectionLimiter::new(RateLimitConfig {
            max_per_second: 10.0,
            burst_size: 2,
        });

        let r1 = route_message(detection_msg("node-1"), None, Some(&limiter)).unwrap();
        let r2 = route_message(detection_msg("node-1"), None, Some(&limiter)).unwrap();
        let r3 = route_message(detection_msg("node-1"), None, Some(&limiter)).unwrap();

        assert!(
            matches!(r1, SapientUpdate::Detected { .. }),
            "1st should be Detected"
        );
        assert!(
            matches!(r2, SapientUpdate::Detected { .. }),
            "2nd should be Detected"
        );
        assert!(
            matches!(&r3, SapientUpdate::Ignored { reason } if reason.contains("rate-limited")),
            "3rd should be Ignored (rate-limited)"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn detections_resume_after_token_refill() {
        use crate::rate_limit::{DetectionLimiter, RateLimitConfig};
        use std::time::Duration;
        let limiter = DetectionLimiter::new(RateLimitConfig {
            max_per_second: 10.0,
            burst_size: 1,
        });

        let r1 = route_message(detection_msg("node-1"), None, Some(&limiter)).unwrap();
        let r2 = route_message(detection_msg("node-1"), None, Some(&limiter)).unwrap();
        assert!(matches!(r1, SapientUpdate::Detected { .. }));
        assert!(
            matches!(&r2, SapientUpdate::Ignored { .. }),
            "should be dropped"
        );

        tokio::time::advance(Duration::from_millis(100)).await;

        let r3 = route_message(detection_msg("node-1"), None, Some(&limiter)).unwrap();
        assert!(
            matches!(r3, SapientUpdate::Detected { .. }),
            "should pass after refill"
        );
    }

    #[test]
    fn no_limiter_forwards_all_detections() {
        for i in 0..5 {
            let r = route_message(detection_msg(&format!("node-{i}")), None, None).unwrap();
            assert!(
                matches!(r, SapientUpdate::Detected { .. }),
                "detection {i} should be Detected with no limiter"
            );
        }
    }

    // --- extract_task_id ---

    #[test]
    fn extract_task_id_from_valid_task_message() {
        use crate::proto::sapient_msg::bsi_flex_335_v2_0::{task::Control, Task};
        let msg = SapientMessage {
            content: Some(Content::Task(Task {
                task_id: Some("ulid-abc".into()),
                control: Some(Control::Start as i32),
                ..Default::default()
            })),
            ..Default::default()
        };
        assert_eq!(extract_task_id(&msg).unwrap(), "ulid-abc");
    }

    #[test]
    fn extract_task_id_missing_task_id_returns_err() {
        use crate::proto::sapient_msg::bsi_flex_335_v2_0::{task::Control, Task};
        let msg = SapientMessage {
            content: Some(Content::Task(Task {
                task_id: None,
                control: Some(Control::Start as i32),
                ..Default::default()
            })),
            ..Default::default()
        };
        assert!(matches!(
            extract_task_id(&msg),
            Err(SapientError::MappingError { .. })
        ));
    }

    #[test]
    fn extract_task_id_wrong_content_returns_err() {
        use crate::proto::sapient_msg::bsi_flex_335_v2_0::RegistrationAck;
        let msg = SapientMessage {
            content: Some(Content::RegistrationAck(RegistrationAck::default())),
            ..Default::default()
        };
        assert!(matches!(
            extract_task_id(&msg),
            Err(SapientError::MappingError { .. })
        ));
    }
}
