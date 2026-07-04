//! End-to-end: `PeatSapientTransport` running in DLMM mode dials out to a
//! fake ASM/HLDMM, which sends a raw SAPIENT `DetectionReport` back over
//! that connection; the resulting `tracks` document is queryable from a
//! real (in-memory) `peat_mesh::Node`.
//!
//! Mirror of `hldmm_integration.rs` with the `accept`/`connect` roles
//! swapped — exercises `run_dlmm_connect_loop` specifically (distinct from
//! `run_hldmm_accept_loop`: `connect_with_retry` instead of `accept`, and
//! peer identity is the caller-supplied `peer_node_id` rather than one
//! derived from the peer's `SocketAddr`).

use std::sync::Arc;
use std::time::Duration;

use std::collections::HashMap;

use peat_mesh::sync::types::Document as MeshDocument;
use peat_mesh::sync::{DataSyncBackend, InMemoryBackend, Query};
use peat_mesh::transport::{MeshTransport, NodeId, TranslationContext, Translator};
use peat_mesh::Node;
use peat_mesh_sapient::{PeatSapientTransport, SapientRole, SapientTranslator};
use peat_sapient::connection;
use peat_sapient::proto::sapient_msg::bsi_flex_335_v2_0::detection_report::LocationOneof;
use peat_sapient::proto::sapient_msg::bsi_flex_335_v2_0::Location;
use peat_sapient::proto::{Content, DetectionReport, SapientMessage};
use serde_json::json;

#[tokio::test]
async fn detection_report_over_dlmm_dial_lands_as_tracks_document() {
    let backend: Arc<dyn DataSyncBackend> = Arc::new(InMemoryBackend::new_initialized());
    let node = Arc::new(Node::new(backend));
    let translator = Arc::new(SapientTranslator::new());

    // Act as the fake ASM/HLDMM: bind and keep the listener alive for the
    // whole test (unlike the HLDMM-mode test's `free_local_addr`, this
    // listener is the test's own server, not something PeatSapientTransport
    // binds itself — no TOCTOU window here).
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind fake ASM listener");
    let remote_addr = listener.local_addr().expect("local_addr");

    let transport = PeatSapientTransport::new(
        SapientRole::Dlmm {
            remote_addr,
            peer_node_id: NodeId::from("asm-1"),
        },
        node.clone(),
        translator,
    );
    transport.start().await.expect("start");

    // Accept the connection Peat's DLMM-mode dialer establishes, then send
    // one DetectionReport down it as the fake ASM.
    let (mut framed, _peer_addr) = connection::accept(&listener)
        .await
        .expect("fake ASM accept");
    let msg = SapientMessage {
        timestamp: None,
        node_id: Some("sensor-2".into()),
        destination_id: None,
        content: Some(Content::DetectionReport(DetectionReport {
            object_id: Some("det-abc".into()),
            location_oneof: Some(LocationOneof::Location(Location {
                x: Some(-122.42),
                y: Some(37.77),
                z: Some(5.0),
                coordinate_system: Some(1), // LatLngDegM
                ..Default::default()
            })),
            ..Default::default()
        })),
        additional_information: None,
    };
    connection::send(&mut framed, msg).await.expect("send");

    // The recv loop runs on a spawned task; poll rather than assume
    // immediate delivery.
    let mut landed = Vec::new();
    for _ in 0..50 {
        landed = node.query("tracks", &Query::All).await.expect("query");
        if !landed.is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    assert_eq!(
        landed.len(),
        1,
        "expected exactly one tracks document to land"
    );
    assert_eq!(
        landed[0].fields.get("lat").and_then(|v| v.as_f64()),
        Some(37.77)
    );
    assert_eq!(
        landed[0].fields.get("lon").and_then(|v| v.as_f64()),
        Some(-122.42)
    );

    // The peer registered under the caller-supplied peer_node_id, not a
    // SocketAddr-derived one — the thing this test exists to distinguish
    // from the HLDMM-mode path.
    assert_eq!(transport.peer_count(), 1);
    assert!(transport.connected_peers().contains(&NodeId::from("asm-1")));
}

/// Outbound fan-out: a mesh `tracks` document encoded by `SapientTranslator`
/// is sent through the `SapientOutboundSink` and arrives at the connected
/// HLDMM as a SAPIENT `DetectionReport`. This is the CoT → SAPIENT path:
/// CoT XML lands as a mesh doc (via `CotTranslator`), the `TransportManager`
/// fan-out encodes it for SAPIENT (via `SapientTranslator::encode_outbound`),
/// and the sink delivers it over the existing DLMM→HLDMM TCP connection.
#[tokio::test]
async fn outbound_tracks_doc_arrives_as_detection_report_at_hldmm() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind fake HLDMM listener");
    let remote_addr = listener.local_addr().expect("local_addr");

    let backend: Arc<dyn DataSyncBackend> = Arc::new(InMemoryBackend::new_initialized());
    let node = Arc::new(Node::new(backend));
    let translator = Arc::new(SapientTranslator::new());

    let transport = PeatSapientTransport::new(
        SapientRole::Dlmm {
            remote_addr,
            peer_node_id: NodeId::from("hldmm-1"),
        },
        node.clone(),
        translator.clone(),
    );

    let sink = transport.outbound_sink();
    transport.start().await.expect("start");

    let (mut framed, _peer_addr) = connection::accept(&listener)
        .await
        .expect("fake HLDMM accept");

    // Build a CoT-like tracks doc (the fields CotTranslator would produce).
    let fields = HashMap::from([
        ("lat".to_string(), json!(51.5074)),
        ("lon".to_string(), json!(-0.1278)),
        ("hae".to_string(), json!(30.0)),
        ("cot_type".to_string(), json!("a-f-G-U-C")),
        ("callsign".to_string(), json!("BRAVO-2")),
    ]);
    let doc = MeshDocument::with_id("cot-track-001".to_string(), fields);

    let ctx = TranslationContext::outbound().with_collection("tracks");
    let bytes = translator
        .encode_outbound(&doc, &ctx)
        .await
        .expect("encode_outbound should produce bytes for a tracks doc");

    sink.send_outbound(bytes, &ctx)
        .await
        .expect("send_outbound");

    // The outbound channel + tokio::select! loop deliver the message
    // asynchronously; poll with a short timeout.
    let mut received = None;
    for _ in 0..50 {
        match tokio::time::timeout(Duration::from_millis(20), connection::recv(&mut framed)).await {
            Ok(Ok(Some(msg))) => {
                received = Some(msg);
                break;
            }
            Ok(Ok(None)) => panic!("fake HLDMM connection closed unexpectedly"),
            Ok(Err(err)) => panic!("recv error: {err}"),
            Err(_) => continue, // timeout, retry
        }
    }

    let msg = received.expect("HLDMM should have received a message");
    match msg.content {
        Some(Content::DetectionReport(dr)) => {
            assert_eq!(dr.object_id.as_deref(), Some("cot-track-001"));
            match dr.location_oneof {
                Some(LocationOneof::Location(loc)) => {
                    assert!((loc.y.unwrap() - 51.5074).abs() < 1e-6, "lat mismatch");
                    assert!((loc.x.unwrap() - (-0.1278)).abs() < 1e-6, "lon mismatch");
                    assert!((loc.z.unwrap() - 30.0).abs() < 1e-6, "hae mismatch");
                }
                other => panic!("expected Location, got {other:?}"),
            }
        }
        other => panic!("expected DetectionReport, got {other:?}"),
    }
}
