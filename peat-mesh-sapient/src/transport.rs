//! `PeatSapientTransport` — owns the actual SAPIENT TCP connection(s) and
//! drives [`SapientTranslator`] against a [`peat_mesh::Node`].
//!
//! Mirrors `peat-mesh/src/transport/btle.rs`'s `PeatBleTransport` shape:
//! a `MeshTransport`/`Transport` impl wrapping an existing protocol crate's
//! own connection machinery (`peat_sapient::connection`), reused untouched.
//!
//! **Topology mismatch, documented rather than papered over:** unlike BLE
//! (multi-peer, discoverable, dial-by-ID), SAPIENT is fixed-topology — an
//! HLDMM listens and DLMMs connect to it, or a DLMM dials exactly one
//! configured ASM/HLDMM. `connect()`/`disconnect()` below reflect that: they
//! manage the *tracked peer record*, not an on-demand dial, because SAPIENT
//! has no "connect to arbitrary peer by ID" operation. The real connection
//! lifecycle is driven by [`start()`](MeshTransport::start) spawning the
//! accept loop (HLDMM) or the single `connect_with_retry` loop (DLMM).
//!
//! `send_to` is intentionally left at `MeshTransport`'s default
//! (`Err("send not implemented")`) — v1's `SapientTranslator::encode_outbound`
//! always declines (see its module docs), so nothing should ever call
//! `send_to` for the `tracks`/`platforms` collections this transport carries.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Instant;

use async_trait::async_trait;
use prost::Message as _;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{debug, warn};

use peat_mesh::transport::{
    ConnectionHealth, ConnectionState, DisconnectReason, MeshConnection, MeshTransport, NodeId,
    PeerEvent, PeerEventReceiver, Result, TranslationContext, Translator, Transport,
    TransportCapabilities, TransportError, TransportType, PEER_EVENT_CHANNEL_CAPACITY,
};
use peat_mesh::Node as MeshNode;
use peat_sapient::connection::{self, ReconnectConfig, SapientFramed};
use peat_sapient::proto::{Content, SapientMessage};

use crate::translator::SapientTranslator;

const SAPIENT_ORIGIN: &str = "sapient";
/// `TransportType::Custom` tag for SAPIENT — no built-in variant fits a
/// point-to-point TCP protocol bridge; "SP" ASCII-packed, arbitrary but
/// stable within this crate.
const SAPIENT_TRANSPORT_TYPE_TAG: u32 = 0x5350;

/// How this transport is wired into the SAPIENT topology.
#[derive(Debug, Clone)]
pub enum SapientRole {
    /// Accept inbound DLMM connections — Peat acts as the HLDMM.
    Hldmm { listen_addr: SocketAddr },
    /// Dial a single ASM/HLDMM — Peat relays one existing SAPIENT DLMM's
    /// data onto the mesh. `peer_node_id` is the mesh-side identity
    /// assigned to that one peer (SAPIENT itself has no concept of the
    /// mesh's `NodeId`).
    Dlmm {
        remote_addr: SocketAddr,
        peer_node_id: NodeId,
    },
}

struct PeerRecord {
    connected_at: Instant,
    alive: Arc<AtomicBool>,
    recv_task: JoinHandle<()>,
}

/// Snapshot handed out by `get_connection` — cheap to clone, doesn't hold
/// the recv task's `JoinHandle`.
struct PeerRecordHandle {
    peer_id: NodeId,
    connected_at: Instant,
    alive: Arc<AtomicBool>,
}

impl MeshConnection for PeerRecordHandle {
    fn peer_id(&self) -> &NodeId {
        &self.peer_id
    }

    fn is_alive(&self) -> bool {
        self.alive.load(Ordering::Relaxed)
    }

    fn connected_at(&self) -> Instant {
        self.connected_at
    }
}

type PeerMap = Arc<RwLock<HashMap<NodeId, PeerRecord>>>;
type EventSenders = Arc<RwLock<Vec<mpsc::Sender<PeerEvent>>>>;

/// `MeshTransport`/`Transport` impl for SAPIENT (BSI Flex 335 v2.0).
pub struct PeatSapientTransport {
    role: SapientRole,
    translator: Arc<SapientTranslator>,
    node: Arc<MeshNode>,
    peers: PeerMap,
    event_senders: EventSenders,
    started: RwLock<Option<Instant>>,
    listener_task: RwLock<Option<JoinHandle<()>>>,
    capabilities: TransportCapabilities,
}

impl PeatSapientTransport {
    pub fn new(role: SapientRole, node: Arc<MeshNode>, translator: Arc<SapientTranslator>) -> Self {
        Self {
            role,
            translator,
            node,
            peers: Arc::new(RwLock::new(HashMap::new())),
            event_senders: Arc::new(RwLock::new(Vec::new())),
            started: RwLock::new(None),
            listener_task: RwLock::new(None),
            // `..Default::default()` deliberately, not an exhaustive struct
            // literal: `TransportCapabilities` has picked up fields between
            // published `peat-mesh` rc's before (e.g. `beacon_capable`), and
            // exhaustive construction breaks the moment the field set drifts
            // in either direction. Only override what this transport
            // actually differs on from the default.
            capabilities: TransportCapabilities {
                transport_type: TransportType::Custom(SAPIENT_TRANSPORT_TYPE_TAG),
                max_bandwidth_bps: 0,
                typical_latency_ms: 20,
                max_range_meters: 0,
                bidirectional: true,
                reliable: true,
                battery_impact: 0,
                ..Default::default()
            },
        }
    }

    fn emit_event(&self, event: PeerEvent) {
        let senders = self.event_senders.read().unwrap_or_else(|e| e.into_inner());
        for sender in senders.iter() {
            let _ = sender.try_send(event.clone());
        }
    }

    /// Collection this decoded `SapientMessage` belongs to, per its `content`
    /// oneof discriminant. `Translator::decode_inbound`'s return type carries
    /// only a `Document`, not its collection — per the trait's own docs, "the
    /// codec owns the type→collection mapping"; here the transport already
    /// holds the pre-encode `SapientMessage`, so it reads the discriminant
    /// directly rather than re-deriving it from bytes.
    fn collection_for(msg: &SapientMessage) -> Option<&'static str> {
        match msg.content {
            Some(Content::DetectionReport(_)) => Some("tracks"),
            Some(Content::Registration(_)) | Some(Content::StatusReport(_)) => Some("platforms"),
            _ => None,
        }
    }

    /// Register a newly-established peer and emit `PeerEvent::Connected`.
    fn register_peer(
        peers: &PeerMap,
        event_senders: &EventSenders,
        peer_id: NodeId,
        connected_at: Instant,
        alive: Arc<AtomicBool>,
        recv_task: JoinHandle<()>,
    ) {
        peers.write().unwrap_or_else(|e| e.into_inner()).insert(
            peer_id.clone(),
            PeerRecord {
                connected_at,
                alive,
                recv_task,
            },
        );
        let senders = event_senders.read().unwrap_or_else(|e| e.into_inner());
        for sender in senders.iter() {
            let _ = sender.try_send(PeerEvent::Connected {
                peer_id: peer_id.clone(),
                connected_at,
            });
        }
    }

    /// Drive one accepted/connected SAPIENT peer until it disconnects or
    /// errors: receive, decode via the translator, publish to the mesh.
    async fn run_peer_recv_loop(
        mut framed: SapientFramed,
        peer_id: NodeId,
        translator: Arc<SapientTranslator>,
        node: Arc<MeshNode>,
        alive: Arc<AtomicBool>,
    ) {
        loop {
            match connection::recv(&mut framed).await {
                Ok(Some(msg)) => {
                    let Some(collection) = Self::collection_for(&msg) else {
                        continue; // Task/TaskAck/Alert/Error — out of v1 scope, not this transport's concern.
                    };
                    let bytes = msg.encode_to_vec();
                    let ctx = TranslationContext::inbound(peer_id.as_str().to_string());
                    match translator.decode_inbound(&bytes, &ctx).await {
                        Ok(Some(doc)) => {
                            if let Err(err) = node
                                .publish_with_origin(
                                    collection,
                                    doc,
                                    Some(SAPIENT_ORIGIN.to_string()),
                                )
                                .await
                            {
                                warn!(peer = %peer_id, %err, "sapient: publish_with_origin failed");
                            }
                        }
                        Ok(None) => {} // well-formed, not carried — normal traffic
                        Err(err) => {
                            warn!(peer = %peer_id, %err, "sapient: decode_inbound failed");
                        }
                    }
                }
                Ok(None) => {
                    debug!(peer = %peer_id, "sapient: peer closed connection");
                    break;
                }
                Err(err) => {
                    warn!(peer = %peer_id, %err, "sapient: recv error, dropping connection");
                    break;
                }
            }
        }
        alive.store(false, Ordering::Relaxed);
    }

    async fn run_hldmm_accept_loop(
        listen_addr: SocketAddr,
        translator: Arc<SapientTranslator>,
        node: Arc<MeshNode>,
        peers: PeerMap,
        event_senders: EventSenders,
    ) {
        let listener = match TcpListener::bind(listen_addr).await {
            Ok(l) => l,
            Err(err) => {
                warn!(%err, %listen_addr, "sapient: HLDMM listener bind failed, accept loop exiting");
                return;
            }
        };
        loop {
            match connection::accept(&listener).await {
                Ok((framed, addr)) => {
                    let peer_id = NodeId::from(addr.to_string());
                    let alive = Arc::new(AtomicBool::new(true));
                    let connected_at = Instant::now();
                    let recv_task = tokio::spawn(Self::run_peer_recv_loop(
                        framed,
                        peer_id.clone(),
                        translator.clone(),
                        node.clone(),
                        alive.clone(),
                    ));
                    Self::register_peer(
                        &peers,
                        &event_senders,
                        peer_id,
                        connected_at,
                        alive,
                        recv_task,
                    );
                }
                Err(err) => {
                    warn!(%err, "sapient: accept failed, listener loop continuing");
                }
            }
        }
    }

    async fn run_dlmm_connect_loop(
        remote_addr: SocketAddr,
        peer_node_id: NodeId,
        translator: Arc<SapientTranslator>,
        node: Arc<MeshNode>,
        peers: PeerMap,
        event_senders: EventSenders,
    ) {
        let framed =
            match connection::connect_with_retry(remote_addr, &ReconnectConfig::default()).await {
                Ok(framed) => framed,
                Err(err) => {
                    warn!(%err, %remote_addr, "sapient: DLMM connect_with_retry exhausted");
                    return;
                }
            };
        let alive = Arc::new(AtomicBool::new(true));
        let connected_at = Instant::now();
        let recv_task = tokio::spawn(Self::run_peer_recv_loop(
            framed,
            peer_node_id.clone(),
            translator,
            node,
            alive.clone(),
        ));
        Self::register_peer(
            &peers,
            &event_senders,
            peer_node_id,
            connected_at,
            alive,
            recv_task,
        );
    }
}

#[async_trait]
impl MeshTransport for PeatSapientTransport {
    async fn start(&self) -> Result<()> {
        let task = match self.role.clone() {
            SapientRole::Hldmm { listen_addr } => tokio::spawn(Self::run_hldmm_accept_loop(
                listen_addr,
                self.translator.clone(),
                self.node.clone(),
                self.peers.clone(),
                self.event_senders.clone(),
            )),
            SapientRole::Dlmm {
                remote_addr,
                peer_node_id,
            } => tokio::spawn(Self::run_dlmm_connect_loop(
                remote_addr,
                peer_node_id,
                self.translator.clone(),
                self.node.clone(),
                self.peers.clone(),
                self.event_senders.clone(),
            )),
        };
        *self
            .listener_task
            .write()
            .unwrap_or_else(|e| e.into_inner()) = Some(task);
        *self.started.write().unwrap_or_else(|e| e.into_inner()) = Some(Instant::now());
        Ok(())
    }

    async fn stop(&self) -> Result<()> {
        if let Some(task) = self
            .listener_task
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .take()
        {
            task.abort();
        }
        let mut peers = self.peers.write().unwrap_or_else(|e| e.into_inner());
        for (_, record) in peers.drain() {
            record.recv_task.abort();
        }
        *self.started.write().unwrap_or_else(|e| e.into_inner()) = None;
        Ok(())
    }

    async fn connect(&self, peer_id: &NodeId) -> Result<Box<dyn MeshConnection>> {
        // SAPIENT has no dial-by-ID operation (see module docs) — this
        // returns the already-established record if `start()`'s accept/
        // connect loop has produced one, and errors otherwise.
        self.get_connection(peer_id)
            .ok_or_else(|| TransportError::PeerNotFound(peer_id.to_string()))
    }

    async fn disconnect(&self, peer_id: &NodeId) -> Result<()> {
        let record = {
            let mut peers = self.peers.write().unwrap_or_else(|e| e.into_inner());
            peers
                .remove(peer_id)
                .ok_or_else(|| TransportError::PeerNotFound(peer_id.to_string()))?
        };
        let connection_duration = record.connected_at.elapsed();
        record.recv_task.abort();
        self.emit_event(PeerEvent::Disconnected {
            peer_id: peer_id.clone(),
            reason: DisconnectReason::LocalClosed,
            connection_duration,
        });
        Ok(())
    }

    fn get_connection(&self, peer_id: &NodeId) -> Option<Box<dyn MeshConnection>> {
        let peers = self.peers.read().unwrap_or_else(|e| e.into_inner());
        let record = peers.get(peer_id)?;
        Some(Box::new(PeerRecordHandle {
            peer_id: peer_id.clone(),
            connected_at: record.connected_at,
            alive: record.alive.clone(),
        }))
    }

    fn peer_count(&self) -> usize {
        self.peers.read().unwrap_or_else(|e| e.into_inner()).len()
    }

    fn connected_peers(&self) -> Vec<NodeId> {
        self.peers
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .keys()
            .cloned()
            .collect()
    }

    fn subscribe_peer_events(&self) -> PeerEventReceiver {
        let (tx, rx) = mpsc::channel(PEER_EVENT_CHANNEL_CAPACITY);
        self.event_senders
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .push(tx);
        rx
    }

    fn get_peer_health(&self, peer_id: &NodeId) -> Option<ConnectionHealth> {
        let conn = self.get_connection(peer_id)?;
        Some(ConnectionHealth {
            state: if conn.is_alive() {
                ConnectionState::Healthy
            } else {
                ConnectionState::Dead
            },
            ..Default::default()
        })
    }
}

impl Transport for PeatSapientTransport {
    fn capabilities(&self) -> &TransportCapabilities {
        &self.capabilities
    }

    fn is_available(&self) -> bool {
        self.started
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .is_some()
    }

    fn can_reach(&self, peer_id: &NodeId) -> bool {
        self.is_connected(peer_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use peat_mesh::sync::InMemoryBackend;

    /// Register-then-disconnect without a real TCP connection: uses the
    /// crate-private `register_peer` directly (same shape `start()`'s
    /// accept/connect loops use) since `disconnect()`'s event emission and
    /// `subscribe_peer_events`' fan-out don't depend on the wire — only on
    /// the peer bookkeeping, which is what this test exercises. No
    /// end-to-end test covered this path (`hldmm_integration.rs` and
    /// `dlmm_integration.rs` only exercise the happy-path connect+receive).
    fn make_transport() -> PeatSapientTransport {
        let backend: Arc<dyn peat_mesh::sync::DataSyncBackend> =
            Arc::new(InMemoryBackend::new_initialized());
        let node = Arc::new(MeshNode::new(backend));
        let translator = Arc::new(SapientTranslator::new());
        PeatSapientTransport::new(
            SapientRole::Hldmm {
                listen_addr: "127.0.0.1:0".parse().unwrap(),
            },
            node,
            translator,
        )
    }

    #[tokio::test]
    async fn disconnect_emits_disconnected_event() {
        let transport = make_transport();
        let mut events = transport.subscribe_peer_events();

        let peer_id = NodeId::from("peer-1");
        PeatSapientTransport::register_peer(
            &transport.peers,
            &transport.event_senders,
            peer_id.clone(),
            Instant::now(),
            Arc::new(AtomicBool::new(true)),
            tokio::spawn(async {}),
        );
        // Drain the Connected event register_peer emits so it isn't
        // mistaken for the Disconnected event under test.
        assert!(matches!(
            events.recv().await,
            Some(PeerEvent::Connected { .. })
        ));

        transport.disconnect(&peer_id).await.expect("disconnect");

        match events.recv().await {
            Some(PeerEvent::Disconnected {
                peer_id: disconnected_id,
                reason,
                ..
            }) => {
                assert_eq!(disconnected_id, peer_id);
                assert!(matches!(reason, DisconnectReason::LocalClosed));
            }
            other => panic!("expected Disconnected event, got {other:?}"),
        }

        assert_eq!(
            transport.peer_count(),
            0,
            "disconnect must remove the peer record"
        );
    }

    #[tokio::test]
    async fn disconnect_unknown_peer_errors() {
        let transport = make_transport();
        let result = transport.disconnect(&NodeId::from("never-connected")).await;
        assert!(matches!(result, Err(TransportError::PeerNotFound(_))));
    }

    #[tokio::test]
    async fn subscribe_peer_events_fans_out_to_multiple_subscribers() {
        let transport = make_transport();
        let mut first = transport.subscribe_peer_events();
        let mut second = transport.subscribe_peer_events();

        PeatSapientTransport::register_peer(
            &transport.peers,
            &transport.event_senders,
            NodeId::from("peer-2"),
            Instant::now(),
            Arc::new(AtomicBool::new(true)),
            tokio::spawn(async {}),
        );

        for events in [&mut first, &mut second] {
            assert!(
                matches!(events.recv().await, Some(PeerEvent::Connected { .. })),
                "every subscriber must independently receive the event"
            );
        }
    }
}
