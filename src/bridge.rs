//! `SapientBridge` — routes inbound SAPIENT messages to peat-schema updates.
//!
//! Phase 4 will wire `start()` to the live TCP connection; for now `route_message`
//! is a pure, synchronously testable function that owns all mapping logic.

use peat_schema::{
    capability::v1::CapabilityAdvertisement,
    node::v1::NodeState,
    track::v1::{Track, TrackPosition},
};
use tracing::warn;

use crate::{
    error::SapientError,
    proto::sapient_msg::bsi_flex_335_v2_0::{sapient_message::Content, SapientMessage},
    transform::{detection, registration, status},
};

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
    /// Message was received but has no peat mapping (e.g. TaskAck, AlertAck, Task).
    Ignored { reason: String },
}

/// Bridge configuration.
#[derive(Debug, Clone)]
pub struct BridgeConfig {
    /// UUID this bridge presents as its SAPIENT node_id.
    pub node_id: String,
    /// TCP address to listen on (HLDMM mode) or connect to (DLMM mode).
    pub addr: std::net::SocketAddr,
}

/// Bridge state and entry points.
pub struct SapientBridge {
    pub config: BridgeConfig,
}

impl SapientBridge {
    pub fn new(config: BridgeConfig) -> Self {
        Self { config }
    }

    /// Phase 4 stub — will start the TCP listener and pump `route_message` in a task.
    pub async fn start(&self) -> Result<(), SapientError> {
        todo!("Phase 4: TCP lifecycle")
    }
}

/// Route a single inbound SAPIENT message to a `SapientUpdate`.
///
/// `sensor_position` — if `Some`, used to resolve range-bearing detections.
/// Passing `None` causes `UnsupportedCoordinateSystem` for range-bearing reports.
///
/// All unhandled `Content` variants produce `SapientUpdate::Ignored` so that
/// unexpected messages never panic the bridge loop.
pub fn route_message(
    msg: SapientMessage,
    sensor_position: Option<&TrackPosition>,
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
            let track = detection::from_detection_report(&node_id, sensor_position, &dr)?;
            Ok(SapientUpdate::Detected { node_id, track })
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
        let update = route_message(msg_with("node-1", Content::Registration(reg)), None).unwrap();
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
        let update =
            route_message(msg_with("sensor-uuid", Content::Registration(reg)), None).unwrap();
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
        let update = route_message(msg_with("node-2", Content::StatusReport(sr)), None).unwrap();
        assert!(matches!(update, SapientUpdate::StatusUpdated { .. }));
    }

    #[test]
    fn status_updated_node_id_matches() {
        let sr = StatusReport {
            system: Some(System::Warning as i32),
            ..Default::default()
        };
        let update = route_message(msg_with("my-sensor", Content::StatusReport(sr)), None).unwrap();
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
        let update =
            route_message(msg_with("sensor-1", Content::DetectionReport(dr)), None).unwrap();
        assert!(matches!(update, SapientUpdate::Detected { .. }));
    }

    // --- Ignored variants ---

    #[test]
    fn task_ack_routes_to_ignored() {
        use crate::proto::sapient_msg::bsi_flex_335_v2_0::TaskAck;
        let update =
            route_message(msg_with("n", Content::TaskAck(TaskAck::default())), None).unwrap();
        assert!(matches!(update, SapientUpdate::Ignored { .. }));
    }

    #[test]
    fn registration_ack_routes_to_ignored() {
        use crate::proto::sapient_msg::bsi_flex_335_v2_0::RegistrationAck;
        let update = route_message(
            msg_with("n", Content::RegistrationAck(RegistrationAck::default())),
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
        let update = route_message(msg, None).unwrap();
        assert!(matches!(update, SapientUpdate::Ignored { .. }));
    }

    #[test]
    fn ignored_does_not_panic_on_unknown_content() {
        use crate::proto::sapient_msg::bsi_flex_335_v2_0::AlertAck;
        let update =
            route_message(msg_with("n", Content::AlertAck(AlertAck::default())), None).unwrap();
        assert!(matches!(update, SapientUpdate::Ignored { .. }));
    }
}
