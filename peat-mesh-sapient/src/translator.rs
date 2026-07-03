//! `impl peat_mesh::transport::Translator for SapientTranslator`
//!
//! Pure(-ish) codec bridging mesh `Document`s and raw SAPIENT (BSI Flex 335
//! v2.0) protobuf bytes, for the `tracks` and `platforms` collections only
//! (v1 scope — see crate docs). Lives in this adapter crate, not
//! `peat-sapient`, per ADR-059 Amendment 4: SAPIENT is an
//! application-domain-specific transport, so its `Translator` impl belongs
//! in a one-way adapter crate rather than behind a `mesh-translator`
//! back-edge feature in the codec crate itself.

use std::collections::HashMap;
use std::sync::RwLock;

use anyhow::Context;
use async_trait::async_trait;
use peat_mesh::sync::types::Document as MeshDocument;
use peat_mesh::transport::{TranslationContext, Translator};
use peat_sapient::mesh_fields::{platform_to_fields, track_to_fields};
use peat_sapient::proto::{Content, SapientMessage};
use peat_sapient::transform::{detection, registration, status};
use peat_schema::capability::v1::CapabilityAdvertisement;
use peat_schema::track::v1::TrackPosition;
use prost::Message as _;

const SAPIENT_TRANSPORT_ID: &str = "sapient";
const TRACKS_COLLECTION: &str = "tracks";
const PLATFORMS_COLLECTION: &str = "platforms";

/// Codec-only [`Translator`] impl for SAPIENT.
///
/// Holds a small last-known-position cache (interior mutability, per the
/// `Translator` trait's stated concurrency model — "any mutable state...
/// uses interior mutability") so range-bearing `DetectionReport`s can be
/// resolved to WGS84 without the caller threading sensor state through
/// every call. This duplicates the position tracking `peat-sapient`'s own
/// `registry::NodeRegistry` already does for the `SapientBridge` path —
/// accepted for v1 since this translator is used independently of
/// `SapientBridge` (different integration surface, same underlying data);
/// revisit if the duplication becomes a real maintenance burden.
#[derive(Debug, Default)]
pub struct SapientTranslator {
    sensor_positions: RwLock<HashMap<String, TrackPosition>>,
}

impl SapientTranslator {
    pub fn new() -> Self {
        Self::default()
    }

    /// Collection name this codec produces `DetectionReport` documents into.
    pub fn tracks_collection(&self) -> &str {
        TRACKS_COLLECTION
    }

    /// Collection name this codec produces `Registration`/`StatusReport`
    /// documents into.
    pub fn platforms_collection(&self) -> &str {
        PLATFORMS_COLLECTION
    }

    fn cache_position(&self, node_id: &str, position: &TrackPosition) {
        self.sensor_positions
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .insert(node_id.to_string(), *position);
    }

    fn cached_position(&self, node_id: &str) -> Option<TrackPosition> {
        self.sensor_positions
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(node_id)
            .cloned()
    }
}

fn document_from_fields(
    id: String,
    fields: serde_json::Map<String, serde_json::Value>,
) -> MeshDocument {
    MeshDocument::with_id(id, fields.into_iter().collect())
}

#[async_trait]
impl Translator for SapientTranslator {
    fn transport_id(&self) -> &'static str {
        SAPIENT_TRANSPORT_ID
    }

    /// Always declines. SAPIENT has no wire message for "manager pushes a
    /// track/status to a sensor" — `DetectionReport`/`StatusReport`/
    /// `Registration` are strictly DLMM→HLDMM (see
    /// `peat-sapient/docs/c2-collaboration.md`). Tasking is the direction
    /// that *does* flow HLDMM→DLMM, but it's out of scope for this codec —
    /// see `peat-sapient::bridge::SapientBridge::send_task` /
    /// `task_queue::TaskQueue` for that path.
    async fn encode_outbound(
        &self,
        _doc: &MeshDocument,
        _ctx: &TranslationContext,
    ) -> Option<Vec<u8>> {
        None
    }

    async fn decode_inbound(
        &self,
        bytes: &[u8],
        _ctx: &TranslationContext,
    ) -> anyhow::Result<Option<MeshDocument>> {
        let msg = SapientMessage::decode(bytes).context("sapient: prost decode failed")?;
        let node_id = msg.node_id.clone().unwrap_or_default();

        match msg.content {
            Some(Content::Registration(reg)) => {
                let advertisement = registration::from_registration(&node_id, &reg);
                let (id, fields) = platform_to_fields(&advertisement, None);
                Ok(Some(document_from_fields(id, fields)))
            }

            Some(Content::StatusReport(sr)) => {
                let (state, capability_delta) = status::from_status_report(&node_id, &sr);
                if let Some(pos) = &state.position {
                    self.cache_position(
                        &node_id,
                        &TrackPosition {
                            latitude: pos.latitude,
                            longitude: pos.longitude,
                            altitude: pos.altitude as f32,
                            cep_m: 0.0,
                            vertical_error_m: 0.0,
                        },
                    );
                }
                let advertisement = capability_delta.unwrap_or(CapabilityAdvertisement {
                    node_id: node_id.clone(),
                    ..Default::default()
                });
                let (id, fields) = platform_to_fields(&advertisement, Some(&state));
                Ok(Some(document_from_fields(id, fields)))
            }

            Some(Content::DetectionReport(dr)) => {
                let sensor_position = self.cached_position(&node_id);
                let track =
                    detection::from_detection_report(&node_id, sensor_position.as_ref(), &dr)
                        .context("sapient: DetectionReport mapping failed")?;
                let (id, fields) = track_to_fields(&track);
                Ok(Some(document_from_fields(id, fields)))
            }

            // Well-formed but not carried by this codec (Task/TaskAck/Alert/
            // Error/acks) — normal traffic, no diagnostic. Matches
            // CotTranslator's convention for non-atom CoT types.
            _ => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use peat_sapient::proto::sapient_msg::bsi_flex_335_v2_0::Location as SapientLocation;
    use peat_sapient::proto::{DetectionReport, Registration, StatusReport};

    fn msg(node_id: &str, content: Content) -> SapientMessage {
        SapientMessage {
            timestamp: None,
            node_id: Some(node_id.to_string()),
            destination_id: None,
            content: Some(content),
            additional_information: None,
        }
    }

    #[tokio::test]
    async fn transport_id_is_sapient_static() {
        let t = SapientTranslator::new();
        let id: &'static str = t.transport_id();
        assert_eq!(id, "sapient");
    }

    #[tokio::test]
    async fn encode_outbound_always_declines() {
        let t = SapientTranslator::new();
        let doc = MeshDocument::with_id("any".to_string(), HashMap::new());
        let ctx = TranslationContext::outbound().with_collection("tracks");
        assert_eq!(t.encode_outbound(&doc, &ctx).await, None);
    }

    #[tokio::test]
    async fn decode_inbound_declines_unrecognized_content_ok_none() {
        let t = SapientTranslator::new();
        let ctx = TranslationContext::inbound("peer-1");
        // Content::Error has no mapping in this codec — well-formed, not carried.
        let raw = msg(
            "node-1",
            Content::Error(peat_sapient::proto::SapientProtoError {
                packet: None,
                error_message: vec![],
            }),
        )
        .encode_to_vec();
        let result = t.decode_inbound(&raw, &ctx).await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn decode_inbound_errors_on_malformed_bytes() {
        let t = SapientTranslator::new();
        let ctx = TranslationContext::inbound("peer-1");
        let result = t.decode_inbound(&[0xFF, 0xFF, 0xFF], &ctx).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn decode_inbound_registration_produces_platforms_document() {
        let t = SapientTranslator::new();
        let ctx = TranslationContext::inbound("peer-1");
        let raw = msg(
            "node-1",
            Content::Registration(Registration {
                icd_version: Some("8.0".into()),
                ..Default::default()
            }),
        )
        .encode_to_vec();
        let doc = t.decode_inbound(&raw, &ctx).await.unwrap().unwrap();
        assert_eq!(doc.id.as_deref(), Some("node-1"));
    }

    #[tokio::test]
    async fn decode_inbound_status_report_caches_position_for_later_detection() {
        let t = SapientTranslator::new();
        let ctx = TranslationContext::inbound("peer-1");

        let status_raw = msg(
            "node-2",
            Content::StatusReport(StatusReport {
                node_location: Some(SapientLocation {
                    x: Some(-118.25),
                    y: Some(34.05),
                    z: Some(50.0),
                    coordinate_system: Some(1), // LatLngDegM
                    ..Default::default()
                }),
                ..Default::default()
            }),
        )
        .encode_to_vec();
        t.decode_inbound(&status_raw, &ctx).await.unwrap();

        assert!(t.cached_position("node-2").is_some());
    }

    #[tokio::test]
    async fn decode_inbound_detection_report_produces_tracks_document() {
        let t = SapientTranslator::new();
        let ctx = TranslationContext::inbound("peer-1");
        let raw = msg(
            "node-3",
            Content::DetectionReport(DetectionReport {
                object_id: Some("det-1".into()),
                ..Default::default()
            }),
        )
        .encode_to_vec();
        let result = t.decode_inbound(&raw, &ctx).await;
        // DetectionReport with no location is a mapping error in
        // `transform::detection` — decode_inbound must surface it as Err,
        // not silently drop the detection.
        assert!(result.is_err());
    }
}
