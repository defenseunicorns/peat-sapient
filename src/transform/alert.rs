//! SAPIENT `Alert` → `SapientAlertEvent`
//!
//! Decision (#8): `peat_schema::AlertProduct` is semantically wrong for SAPIENT alerts
//! (it models ML-output triggers, not sensor-level severity events). We carry all
//! SAPIENT fields faithfully in `SapientAlertEvent` without a peat-schema dep.
//! A proper peat-schema `Alert` type is a future ADR item.

use peat_schema::track::v1::TrackPosition;

use crate::{
    proto::sapient_msg::bsi_flex_335_v2_0::{
        alert::{AlertStatus, AlertType, DiscretePriority, LocationOneof},
        Alert,
    },
    transform::detection::location_to_track_position,
};

/// Normalised SAPIENT alert — all fields preserved, position coordinate-converted.
///
/// Consumers that need programmatic alert routing should use the string type/status/priority
/// fields until a first-class peat-schema Alert type is added (future ADR).
#[derive(Debug, Clone)]
pub struct SapientAlertEvent {
    pub alert_id: String,
    /// Human-readable alert type label ("Information", "Warning", "Critical", …).
    pub alert_type: String,
    /// Human-readable status label ("Active", "Cleared", …).
    pub status: String,
    /// Human-readable priority label ("Low", "Medium", "High").
    pub priority: String,
    pub description: Option<String>,
    /// Confidence that this is not a false-alarm (0.0–1.0).
    pub confidence: Option<f32>,
    /// Ranking score (0.0–1.0).
    pub ranking: Option<f32>,
    /// Geographic position of the alerting event, coordinate-converted to WGS84.
    pub position: Option<TrackPosition>,
    pub region_id: Option<String>,
    /// JSON bag for remaining SAPIENT fields (associated detections, files, extras).
    pub attributes_json: String,
}

fn alert_type_label(v: i32) -> String {
    AlertType::try_from(v)
        .map(|t| format!("{t:?}"))
        .unwrap_or_else(|_| format!("Unknown({v})"))
}

fn alert_status_label(v: i32) -> String {
    AlertStatus::try_from(v)
        .map(|s| format!("{s:?}"))
        .unwrap_or_else(|_| format!("Unknown({v})"))
}

fn priority_label(v: i32) -> String {
    DiscretePriority::try_from(v)
        .map(|p| format!("{p:?}"))
        .unwrap_or_else(|_| format!("Unknown({v})"))
}

/// Convert a SAPIENT `Alert` to a `SapientAlertEvent`.
///
/// Position is coordinate-converted using the same logic as `detection::location_to_track_position`.
/// An unresolvable coordinate system (e.g. RangeBearing without sensor position) is not an
/// error — the position is silently set to `None` so the caller always gets a usable event.
pub fn from_alert(node_id: &str, msg: &Alert) -> SapientAlertEvent {
    let position = msg.location_oneof.as_ref().and_then(|lo| match lo {
        LocationOneof::Location(loc) => location_to_track_position(loc).ok(),
        LocationOneof::RangeBearing(_) => None,
    });

    let mut attrs = serde_json::Map::new();
    if !msg.associated_detection.is_empty() {
        attrs.insert(
            "associated_detection_ids".into(),
            serde_json::Value::Array(
                msg.associated_detection
                    .iter()
                    .filter_map(|d| {
                        d.object_id
                            .as_ref()
                            .map(|id| serde_json::Value::String(id.clone()))
                    })
                    .collect(),
            ),
        );
    }
    if let Some(extra) = &msg.additional_information {
        attrs.insert(
            "additional_information".into(),
            serde_json::Value::String(extra.clone()),
        );
    }
    if let Some(rid) = &msg.region_id {
        attrs.insert(
            "sapient_region_id".into(),
            serde_json::Value::String(rid.clone()),
        );
    }
    attrs.insert(
        "sapient_source_node_id".into(),
        serde_json::Value::String(node_id.to_string()),
    );

    SapientAlertEvent {
        alert_id: msg.alert_id.clone().unwrap_or_default(),
        alert_type: alert_type_label(msg.alert_type.unwrap_or(0)),
        status: alert_status_label(msg.status.unwrap_or(0)),
        priority: priority_label(msg.priority.unwrap_or(0)),
        description: msg.description.clone(),
        confidence: msg.confidence,
        ranking: msg.ranking,
        position,
        region_id: msg.region_id.clone(),
        attributes_json: serde_json::to_string(&attrs).unwrap_or_default(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::sapient_msg::bsi_flex_335_v2_0::{
        alert::{AlertStatus, AlertType, DiscretePriority, LocationOneof},
        Alert, AssociatedDetection, Location, LocationCoordinateSystem, LocationDatum,
    };

    fn basic_alert() -> Alert {
        Alert {
            alert_id: Some("01HZALERT0000000000000000A".into()),
            alert_type: Some(AlertType::Warning as i32),
            status: Some(AlertStatus::Active as i32),
            priority: Some(DiscretePriority::High as i32),
            description: Some("Perimeter breach detected".into()),
            confidence: Some(0.92),
            ranking: Some(0.85),
            ..Default::default()
        }
    }

    #[test]
    fn alert_id_preserved() {
        let event = from_alert("node-1", &basic_alert());
        assert_eq!(event.alert_id, "01HZALERT0000000000000000A");
    }

    #[test]
    fn alert_type_warning_label() {
        let event = from_alert("node-1", &basic_alert());
        assert_eq!(event.alert_type, "Warning");
    }

    #[test]
    fn alert_type_critical_label() {
        let mut a = basic_alert();
        a.alert_type = Some(AlertType::Critical as i32);
        let event = from_alert("node-1", &a);
        assert_eq!(event.alert_type, "Critical");
    }

    #[test]
    fn alert_status_active_label() {
        let event = from_alert("node-1", &basic_alert());
        assert_eq!(event.status, "Active");
    }

    #[test]
    fn alert_status_clear_label() {
        let mut a = basic_alert();
        a.status = Some(AlertStatus::Clear as i32);
        let event = from_alert("node-1", &a);
        assert_eq!(event.status, "Clear");
    }

    #[test]
    fn priority_high_label() {
        let event = from_alert("node-1", &basic_alert());
        assert_eq!(event.priority, "High");
    }

    #[test]
    fn priority_low_label() {
        let mut a = basic_alert();
        a.priority = Some(DiscretePriority::Low as i32);
        let event = from_alert("node-1", &a);
        assert_eq!(event.priority, "Low");
    }

    #[test]
    fn description_preserved() {
        let event = from_alert("node-1", &basic_alert());
        assert_eq!(
            event.description.as_deref(),
            Some("Perimeter breach detected")
        );
    }

    #[test]
    fn confidence_preserved() {
        let event = from_alert("node-1", &basic_alert());
        assert!((event.confidence.unwrap() - 0.92).abs() < 1e-5);
    }

    #[test]
    fn no_location_yields_none_position() {
        let event = from_alert("node-1", &basic_alert());
        assert!(event.position.is_none());
    }

    #[test]
    fn latlng_location_converted_to_position() {
        let mut a = basic_alert();
        a.location_oneof = Some(LocationOneof::Location(Location {
            x: Some(-0.1278),
            y: Some(51.5074),
            z: Some(5.0),
            coordinate_system: Some(LocationCoordinateSystem::LatLngDegM as i32),
            datum: Some(LocationDatum::Wgs84E as i32),
            ..Default::default()
        }));
        let event = from_alert("node-1", &a);
        let pos = event.position.unwrap();
        assert!((pos.latitude - 51.5074).abs() < 1e-9);
        assert!((pos.longitude - (-0.1278)).abs() < 1e-9);
    }

    #[test]
    fn range_bearing_location_yields_none_position() {
        use crate::proto::sapient_msg::bsi_flex_335_v2_0::RangeBearing;
        let mut a = basic_alert();
        a.location_oneof = Some(LocationOneof::RangeBearing(RangeBearing {
            range: Some(200.0),
            azimuth: Some(45.0),
            ..Default::default()
        }));
        let event = from_alert("node-1", &a);
        // RangeBearing without sensor position → silently None (not an error)
        assert!(event.position.is_none());
    }

    #[test]
    fn source_node_id_in_attributes_json() {
        let event = from_alert("sensor-uuid-123", &basic_alert());
        let attrs: serde_json::Value = serde_json::from_str(&event.attributes_json).unwrap();
        assert_eq!(attrs["sapient_source_node_id"], "sensor-uuid-123");
    }

    #[test]
    fn associated_detection_ids_in_attributes_json() {
        let mut a = basic_alert();
        a.associated_detection = vec![
            AssociatedDetection {
                object_id: Some("det-001".into()),
                ..Default::default()
            },
            AssociatedDetection {
                object_id: Some("det-002".into()),
                ..Default::default()
            },
        ];
        let event = from_alert("node-1", &a);
        let attrs: serde_json::Value = serde_json::from_str(&event.attributes_json).unwrap();
        let ids = attrs["associated_detection_ids"].as_array().unwrap();
        assert_eq!(ids.len(), 2);
        assert!(ids.contains(&serde_json::Value::String("det-001".into())));
    }

    #[test]
    fn no_alert_id_defaults_to_empty_string() {
        let a = Alert::default();
        let event = from_alert("node-1", &a);
        assert_eq!(event.alert_id, "");
    }
}
