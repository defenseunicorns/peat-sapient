//! Flat-JSON projection helpers for peat-mesh's generic `Document` collections.
//!
//! Behind the `translator-codec` feature. Pure functions, zero `peat-mesh`
//! dependency — the `peat_mesh::transport::Translator` impl that consumes
//! these lives in the separate `peat-mesh-sapient` crate (ADR-059 Amendment 4:
//! wire codec stays in the transport crate, the trait impl lives in a
//! one-way adapter crate for application-domain-specific transports).
//!
//! Field naming mirrors the conventions `CotTranslator`/`BleTranslator`
//! already use for the same collections (`lat`/`lon`/`hae`/`timestamp_ms`
//! for `tracks`) so multiple translators can read the same mesh `Document`
//! without agreeing on anything beyond field names.

use peat_schema::{
    capability::v1::{CapabilityAdvertisement, OperationalStatus},
    node::v1::{HealthStatus, NodeState, Phase},
    track::v1::Track,
};
use serde_json::{json, Map, Value};

/// Project a `Track` (produced by [`crate::transform::detection::to_track`])
/// into the flat field shape the `tracks` collection uses. Returns
/// `(document_id, fields)`.
///
/// Deliberately omits `cot_type`: an autonomous sensor detection carries no
/// affiliation information, so leaving the field unset lets `CotTranslator`
/// apply its own configured default (`a-f-G-U-C`) rather than this codec
/// guessing friendly/hostile/unknown. SAPIENT-specific fields with no CoT
/// equivalent are preserved verbatim under a `sapient_` prefix — the same
/// "opaque extension" precedent ADR-070 established for `Track.extension`.
pub fn track_to_fields(track: &Track) -> (String, Map<String, Value>) {
    let mut fields = Map::new();

    if let Some(pos) = &track.position {
        fields.insert("lat".into(), json!(pos.latitude));
        fields.insert("lon".into(), json!(pos.longitude));
        if pos.altitude != 0.0 {
            fields.insert("hae".into(), json!(pos.altitude));
        }
    }

    if let Some(last_seen) = &track.last_seen {
        let ms = last_seen.seconds.saturating_mul(1000) + (last_seen.nanos / 1_000_000) as u64;
        fields.insert("timestamp_ms".into(), json!(ms));
    }

    if !track.classification.is_empty() {
        fields.insert("sapient_classification".into(), json!(track.classification));
    }
    fields.insert("sapient_confidence".into(), json!(track.confidence));
    if let Some(source) = &track.source {
        if !source.node_id.is_empty() {
            fields.insert("sapient_source_node_id".into(), json!(source.node_id));
        }
    }

    (track.track_id.clone(), fields)
}

/// Project a `CapabilityAdvertisement` (from
/// [`crate::transform::registration::from_registration`]) and an optional
/// `NodeState` delta (from [`crate::transform::status`]) into the flat field
/// shape used for the `platforms` collection. Returns `(document_id, fields)`.
///
/// No CoT-side consumer exists for `platforms` yet — `CotTranslator` only
/// carries `tracks` today — so this shape is new precedent rather than a
/// match against an established convention. Reconcile against `peat-btle`'s
/// platforms producer (`BleTranslator`'s `platforms_collection()` surface,
/// per ADR-059 Amendment 4) once available; it could not be checked directly
/// against this repo's sibling `peat-btle` checkout, which predates that
/// surface.
pub fn platform_to_fields(
    advertisement: &CapabilityAdvertisement,
    state: Option<&NodeState>,
) -> (String, Map<String, Value>) {
    let mut fields = Map::new();

    let capability_names: Vec<String> = advertisement
        .capabilities
        .iter()
        .map(|c| c.name.clone())
        .collect();
    if !capability_names.is_empty() {
        fields.insert("capabilities".into(), json!(capability_names));
    }

    let operational_status = OperationalStatus::try_from(advertisement.operational_status)
        .unwrap_or(OperationalStatus::Unspecified);
    fields.insert(
        "operational_status".into(),
        json!(format!("{operational_status:?}")),
    );

    if let Some(state) = state {
        let health = HealthStatus::try_from(state.health).unwrap_or(HealthStatus::Unspecified);
        let phase = Phase::try_from(state.phase).unwrap_or(Phase::Unspecified);
        fields.insert("health".into(), json!(format!("{health:?}")));
        fields.insert("phase".into(), json!(format!("{phase:?}")));

        if let Some(pos) = &state.position {
            fields.insert("lat".into(), json!(pos.latitude));
            fields.insert("lon".into(), json!(pos.longitude));
            if pos.altitude != 0.0 {
                fields.insert("hae".into(), json!(pos.altitude));
            }
        }
    }

    (advertisement.node_id.clone(), fields)
}

#[cfg(test)]
mod tests {
    use super::*;
    use peat_schema::{
        capability::v1::{Capability, CapabilityType},
        common::v1::{Position, Timestamp},
        node::v1::Phase as NodePhase,
        track::v1::{SourceType, TrackPosition, TrackSource},
    };

    fn sample_track() -> Track {
        Track {
            track_id: "trk-1".into(),
            classification: "vehicle".into(),
            confidence: 0.87,
            position: Some(TrackPosition {
                latitude: 34.05,
                longitude: -118.25,
                altitude: 120.0,
                cep_m: 5.0,
                vertical_error_m: 2.0,
            }),
            velocity: None,
            state: 0,
            source: Some(TrackSource {
                node_id: "sensor-42".into(),
                sensor_id: String::new(),
                model_version: String::new(),
                source_type: SourceType::Sensor as i32,
            }),
            attributes_json: String::new(),
            first_seen: None,
            last_seen: Some(Timestamp {
                seconds: 1_700_000_000,
                nanos: 500_000_000,
            }),
            observation_count: 3,
        }
    }

    #[test]
    fn track_to_fields_projects_position_and_extensions() {
        let (id, fields) = track_to_fields(&sample_track());
        assert_eq!(id, "trk-1");
        assert_eq!(fields["lat"], json!(34.05));
        assert_eq!(fields["lon"], json!(-118.25));
        assert_eq!(fields["hae"], json!(120.0));
        assert_eq!(fields["timestamp_ms"], json!(1_700_000_000_500u64));
        assert_eq!(fields["sapient_classification"], json!("vehicle"));
        assert_eq!(fields["sapient_source_node_id"], json!("sensor-42"));
        assert!(
            fields.get("cot_type").is_none(),
            "must not guess affiliation"
        );
    }

    #[test]
    fn track_to_fields_omits_zero_altitude() {
        let mut track = sample_track();
        track.position.as_mut().unwrap().altitude = 0.0;
        let (_, fields) = track_to_fields(&track);
        assert!(fields.get("hae").is_none());
    }

    #[test]
    fn platform_to_fields_projects_capabilities_and_health() {
        let advertisement = CapabilityAdvertisement {
            node_id: "node-9".into(),
            advertised_at: None,
            capabilities: vec![Capability {
                id: "cap-1".into(),
                name: "radar".into(),
                capability_type: CapabilityType::Sensor as i32,
                confidence: 1.0,
                metadata_json: String::new(),
                registered_at: None,
            }],
            resources: None,
            operational_status: OperationalStatus::Ready as i32,
        };
        let state = NodeState {
            position: Some(Position {
                latitude: 1.0,
                longitude: 2.0,
                altitude: 0.0,
            }),
            fuel_minutes: 0,
            health: HealthStatus::Nominal as i32,
            phase: NodePhase::Hierarchy as i32,
            cell_id: None,
            zone_id: None,
            timestamp: None,
        };

        let (id, fields) = platform_to_fields(&advertisement, Some(&state));
        assert_eq!(id, "node-9");
        assert_eq!(fields["capabilities"], json!(["radar"]));
        assert_eq!(fields["operational_status"], json!("Ready"));
        assert_eq!(fields["health"], json!("Nominal"));
        assert_eq!(fields["phase"], json!("Hierarchy"));
        assert_eq!(fields["lat"], json!(1.0));
        assert!(fields.get("hae").is_none(), "zero altitude omitted");
    }

    #[test]
    fn platform_to_fields_without_state_still_projects_capability_data() {
        let advertisement = CapabilityAdvertisement {
            node_id: "node-10".into(),
            advertised_at: None,
            capabilities: vec![],
            resources: None,
            operational_status: OperationalStatus::Unspecified as i32,
        };
        let (id, fields) = platform_to_fields(&advertisement, None);
        assert_eq!(id, "node-10");
        assert!(fields.get("capabilities").is_none());
        assert!(fields.get("health").is_none());
    }
}
