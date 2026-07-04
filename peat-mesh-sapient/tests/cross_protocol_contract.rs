//! Cross-protocol contract tests: verify that `SapientTranslator` correctly
//! handles mesh documents shaped like what `CotTranslator` produces, and
//! vice versa, WITHOUT importing `peat-transport`.
//!
//! The shared `tracks` schema contract:
//! - `lat` (f64), `lon` (f64), `doc.id` (String) — minimum viable interop
//! - `hae` (f64, optional), `timestamp_ms` (i64 millis, optional)
//! - `cot_type` (String) — CoT-specific, SAPIENT ignores on encode
//! - `callsign` (String) — CoT-specific, SAPIENT ignores on encode
//! - `tak_origin` (bool) — CoT-specific origin marker
//! - `sapient_*` fields — SAPIENT-specific, CoT ignores

use std::collections::HashMap;

use peat_mesh::sync::types::Document as MeshDocument;
use peat_mesh::transport::{TranslationContext, Translator};
use peat_mesh_sapient::SapientTranslator;
use peat_sapient::proto::sapient_msg::bsi_flex_335_v2_0::detection_report::LocationOneof;
use peat_sapient::proto::{Content, SapientMessage};
use prost::Message as _;
use serde_json::json;

fn cot_like_tracks_doc(uid: &str, lat: f64, lon: f64) -> MeshDocument {
    let fields = HashMap::from([
        ("lat".to_string(), json!(lat)),
        ("lon".to_string(), json!(lon)),
        ("hae".to_string(), json!(150.0)),
        ("cot_type".to_string(), json!("a-f-G-U-C")),
        ("callsign".to_string(), json!("ALPHA-1")),
        ("tak_origin".to_string(), json!(true)),
        ("timestamp_ms".to_string(), json!(1719964800000_i64)),
    ]);
    MeshDocument::with_id(uid.to_string(), fields)
}

fn minimal_cot_doc(uid: &str, lat: f64, lon: f64) -> MeshDocument {
    let fields = HashMap::from([
        ("lat".to_string(), json!(lat)),
        ("lon".to_string(), json!(lon)),
    ]);
    MeshDocument::with_id(uid.to_string(), fields)
}

/// A full CoT-shaped document (all fields CotTranslator would produce)
/// encodes to a valid SAPIENT DetectionReport with correct position.
#[tokio::test]
async fn cot_doc_encodes_to_sapient_detection_report() {
    let t = SapientTranslator::new();
    let doc = cot_like_tracks_doc("cot-uid-001", 51.5074, -0.1278);
    let ctx = TranslationContext::outbound().with_collection("tracks");

    let bytes = t.encode_outbound(&doc, &ctx).await.unwrap();
    let msg = SapientMessage::decode(bytes.as_slice()).unwrap();

    match msg.content {
        Some(Content::DetectionReport(dr)) => {
            assert_eq!(dr.object_id.as_deref(), Some("cot-uid-001"));
            match dr.location_oneof {
                Some(LocationOneof::Location(loc)) => {
                    assert!((loc.y.unwrap() - 51.5074).abs() < 1e-6);
                    assert!((loc.x.unwrap() - (-0.1278)).abs() < 1e-6);
                    assert!((loc.z.unwrap() - 150.0).abs() < 1e-6);
                }
                _ => panic!("expected Location"),
            }
        }
        _ => panic!("expected DetectionReport"),
    }
}

/// Minimal CoT doc (only lat, lon, uid) still encodes successfully.
#[tokio::test]
async fn minimal_cot_doc_encodes() {
    let t = SapientTranslator::new();
    let doc = minimal_cot_doc("cot-min-001", 34.05, -118.25);
    let ctx = TranslationContext::outbound().with_collection("tracks");

    let bytes = t.encode_outbound(&doc, &ctx).await;
    assert!(bytes.is_some(), "minimal CoT doc should encode");
}

/// CoT-specific fields (cot_type, callsign, tak_origin) don't interfere
/// with encoding — they're silently ignored.
#[tokio::test]
async fn cot_specific_fields_ignored_gracefully() {
    let t = SapientTranslator::new();
    let doc = cot_like_tracks_doc("cot-uid-002", 51.5, -0.12);
    let ctx = TranslationContext::outbound().with_collection("tracks");

    let bytes = t.encode_outbound(&doc, &ctx).await.unwrap();
    let msg = SapientMessage::decode(bytes.as_slice()).unwrap();
    match msg.content {
        Some(Content::DetectionReport(dr)) => {
            assert!(
                dr.classification.is_empty(),
                "cot_type/callsign should not become classification"
            );
        }
        _ => panic!("expected DetectionReport"),
    }
}

/// Full round-trip: SAPIENT protobuf → decode_inbound → mesh doc →
/// encode_outbound → protobuf. Position must survive.
#[tokio::test]
async fn sapient_round_trip_preserves_position() {
    let t = SapientTranslator::new();

    let original = SapientMessage {
        node_id: Some("sensor-rt".into()),
        content: Some(Content::DetectionReport(
            peat_sapient::proto::DetectionReport {
                object_id: Some("det-rt-001".into()),
                location_oneof: Some(LocationOneof::Location(
                    peat_sapient::proto::sapient_msg::bsi_flex_335_v2_0::Location {
                        x: Some(-0.1278),
                        y: Some(51.5074),
                        z: Some(30.0),
                        coordinate_system: Some(1),
                        ..Default::default()
                    },
                )),
                ..Default::default()
            },
        )),
        ..Default::default()
    };
    let wire_bytes = original.encode_to_vec();

    let ctx_in = TranslationContext::inbound("peer-1");
    let mesh_doc = t
        .decode_inbound(&wire_bytes, &ctx_in)
        .await
        .unwrap()
        .unwrap();

    let ctx_out = TranslationContext::outbound().with_collection("tracks");
    let re_encoded = t.encode_outbound(&mesh_doc, &ctx_out).await.unwrap();
    let roundtripped = SapientMessage::decode(re_encoded.as_slice()).unwrap();

    match roundtripped.content {
        Some(Content::DetectionReport(dr)) => match dr.location_oneof {
            Some(LocationOneof::Location(loc)) => {
                assert!((loc.y.unwrap() - 51.5074).abs() < 1e-4);
                assert!((loc.x.unwrap() - (-0.1278)).abs() < 1e-4);
            }
            _ => panic!("expected Location"),
        },
        _ => panic!("expected DetectionReport"),
    }
}

/// SAPIENT-originated doc round-trips classification through encode→decode.
#[tokio::test]
async fn sapient_classification_survives_round_trip() {
    let t = SapientTranslator::new();

    let mut fields = HashMap::new();
    fields.insert("lat".to_string(), json!(34.05));
    fields.insert("lon".to_string(), json!(-118.25));
    fields.insert("sapient_classification".to_string(), json!("vehicle"));
    fields.insert("sapient_confidence".to_string(), json!(0.92));
    let doc = MeshDocument::with_id("cls-rt-001".to_string(), fields);

    let ctx_out = TranslationContext::outbound().with_collection("tracks");
    let bytes = t.encode_outbound(&doc, &ctx_out).await.unwrap();

    let ctx_in = TranslationContext::inbound("peer-1");
    let decoded_doc = t.decode_inbound(&bytes, &ctx_in).await.unwrap().unwrap();

    assert_eq!(
        decoded_doc
            .fields
            .get("sapient_classification")
            .and_then(|v| v.as_str()),
        Some("vehicle")
    );
    let conf = decoded_doc
        .fields
        .get("sapient_confidence")
        .and_then(|v| v.as_f64())
        .unwrap();
    assert!((conf - 0.92).abs() < 0.01);
}
