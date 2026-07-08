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
//!
//! `timestamp_ms` is always `i64` (milliseconds since Unix epoch) regardless
//! of whether the originating protocol uses signed or unsigned timestamps.
//! This matches `chrono::DateTime::timestamp_millis()` (used by CoT) and
//! covers all practical values; consumers should read via `as_i64()`.

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
        fields.insert("timestamp_ms".into(), json!(ms as i64));
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

/// Stable wire string for `OperationalStatus` — deliberately not `{:?}`.
/// `Debug` output on a prost-generated enum isn't a stable contract: a
/// `.proto` variant rename in `peat-schema` would silently change what
/// lands in the mesh `Document`, with no compile error to catch it.
fn operational_status_str(status: OperationalStatus) -> &'static str {
    match status {
        OperationalStatus::Unspecified => "unspecified",
        OperationalStatus::Ready => "ready",
        OperationalStatus::Active => "active",
        OperationalStatus::Degraded => "degraded",
        OperationalStatus::Offline => "offline",
        OperationalStatus::Maintenance => "maintenance",
    }
}

/// Stable wire string for `HealthStatus` — see [`operational_status_str`].
fn health_status_str(health: HealthStatus) -> &'static str {
    match health {
        HealthStatus::Unspecified => "unspecified",
        HealthStatus::Nominal => "nominal",
        HealthStatus::Degraded => "degraded",
        HealthStatus::Critical => "critical",
        HealthStatus::Failed => "failed",
    }
}

/// Stable wire string for `Phase` — see [`operational_status_str`].
fn phase_str(phase: Phase) -> &'static str {
    match phase {
        Phase::Unspecified => "unspecified",
        Phase::Discovery => "discovery",
        Phase::Cell => "cell",
        Phase::Hierarchy => "hierarchy",
    }
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
    // Omit rather than write "Unspecified": a StatusReport with no FOV/mode
    // change carries `capability_delta = None` (transform::status), so the
    // caller synthesizes an empty CapabilityAdvertisement here — writing
    // "Unspecified" unconditionally would let every such heartbeat clobber
    // a real Ready/Degraded/Offline value a prior Registration set, under
    // peat-mesh's field-level LWW merge. Same "empty means absent" treatment
    // `capabilities` already gets above.
    if operational_status != OperationalStatus::Unspecified {
        fields.insert(
            "operational_status".into(),
            json!(operational_status_str(operational_status)),
        );
    }

    if let Some(state) = state {
        let health = HealthStatus::try_from(state.health).unwrap_or(HealthStatus::Unspecified);
        let phase = Phase::try_from(state.phase).unwrap_or(Phase::Unspecified);
        fields.insert("health".into(), json!(health_status_str(health)));
        fields.insert("phase".into(), json!(phase_str(phase)));

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
            kinematics: None,
            position_error: None,
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
        assert_eq!(fields["timestamp_ms"], json!(1_700_000_000_500i64));
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
            kinematics: None,
            position_error: None,
        };

        let (id, fields) = platform_to_fields(&advertisement, Some(&state));
        assert_eq!(id, "node-9");
        assert_eq!(fields["capabilities"], json!(["radar"]));
        assert_eq!(fields["operational_status"], json!("ready"));
        assert_eq!(fields["health"], json!("nominal"));
        assert_eq!(fields["phase"], json!("hierarchy"));
        assert_eq!(fields["lat"], json!(1.0));
        assert!(fields.get("hae").is_none(), "zero altitude omitted");
    }

    /// The bug the QA review caught: a heartbeat StatusReport with no
    /// FOV/mode change carries an empty, synthesized `CapabilityAdvertisement`
    /// (`operational_status = Unspecified`). Writing that unconditionally
    /// would let every heartbeat clobber a real Ready/Degraded/Offline value
    /// a prior Registration set, under peat-mesh's field-level LWW merge.
    #[test]
    fn platform_to_fields_omits_unspecified_operational_status() {
        let advertisement = CapabilityAdvertisement {
            node_id: "node-11".into(),
            operational_status: OperationalStatus::Unspecified as i32,
            ..Default::default()
        };
        let (_, fields) = platform_to_fields(&advertisement, None);
        assert!(
            fields.get("operational_status").is_none(),
            "Unspecified must be omitted, not written, so it can't clobber a prior known status"
        );
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
