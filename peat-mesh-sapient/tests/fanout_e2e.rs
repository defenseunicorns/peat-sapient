//! Full-stack TransportManager fan-out e2e tests.
//!
//! These tests wire up the complete pipeline:
//!   Node::publish_with_origin → TransportManager observer → fan-out →
//!   SapientTranslator::encode_outbound → SapientOutboundSink → TCP →
//!   fake HLDMM receives DetectionReport
//!
//! This exercises the actual TransportManager integration rather than
//! calling `send_outbound` on the sink directly (which `dlmm_integration.rs`
//! already covers).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use peat_mesh::sync::types::Document as MeshDocument;
use peat_mesh::sync::{DataSyncBackend, InMemoryBackend};
use peat_mesh::transport::{
    MeshTransport, NodeId, TranslatorRegistrationConfig, TransportManager, TransportManagerConfig,
};
use peat_mesh::Node;
use peat_mesh_sapient::{PeatSapientTransport, SapientRole, SapientTranslator};
use peat_sapient::connection;
use peat_sapient::proto::sapient_msg::bsi_flex_335_v2_0::detection_report::LocationOneof;
use peat_sapient::proto::{Content, SapientMessage};
use serde_json::json;

/// Publish a CoT-originated tracks doc on the mesh → TransportManager fan-out
/// encodes it via SapientTranslator → SapientOutboundSink delivers it over the
/// DLMM→HLDMM TCP connection → fake HLDMM receives a DetectionReport.
#[tokio::test]
async fn fanout_cot_doc_reaches_hldmm_as_detection_report() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind fake HLDMM");
    let remote_addr = listener.local_addr().expect("local_addr");

    let backend: Arc<dyn DataSyncBackend> = Arc::new(InMemoryBackend::new_initialized());
    let node = Arc::new(Node::new(backend));
    let translator = Arc::new(SapientTranslator::new());

    let transport = PeatSapientTransport::new(
        SapientRole::Dlmm {
            remote_addr,
            peer_node_id: NodeId::from("hldmm-fanout"),
        },
        node.clone(),
        translator.clone(),
    );

    let sink = transport.outbound_sink();
    transport.start().await.expect("start transport");

    // Accept the DLMM dial-out before setting up fan-out, so the TCP
    // connection is established and the outbound channel is live.
    let (mut framed, _) = connection::accept(&listener).await.expect("accept");

    // Wire up TransportManager fan-out: translator + sink + observer on "tracks".
    let mgr = Arc::new(TransportManager::new(TransportManagerConfig::default()));
    mgr.register_translator(
        translator.clone(),
        sink,
        TranslatorRegistrationConfig::default(),
    )
    .await
    .expect("register_translator");
    let _fanout_handle = mgr
        .start_fanout(node.clone(), vec!["tracks".to_string()])
        .expect("start_fanout");

    // Publish a CoT-like tracks doc with origin "tak" — the fan-out should
    // skip the originator and deliver to SAPIENT (origin != "sapient").
    let fields = HashMap::from([
        ("lat".to_string(), json!(51.5074)),
        ("lon".to_string(), json!(-0.1278)),
        ("hae".to_string(), json!(45.0)),
    ]);
    let doc = MeshDocument::with_id("cot-fanout-001".to_string(), fields);
    node.publish_with_origin("tracks", doc, Some("tak".into()))
        .await
        .expect("publish");

    // Poll until the fake HLDMM receives the forwarded DetectionReport.
    let mut received = None;
    for _ in 0..100 {
        match tokio::time::timeout(Duration::from_millis(20), connection::recv(&mut framed)).await {
            Ok(Ok(Some(msg))) => {
                received = Some(msg);
                break;
            }
            Ok(Ok(None)) => panic!("HLDMM connection closed unexpectedly"),
            Ok(Err(err)) => panic!("recv error: {err}"),
            Err(_) => continue,
        }
    }

    let msg = received.expect("HLDMM should have received the fan-out message");
    match msg.content {
        Some(Content::DetectionReport(dr)) => {
            assert_eq!(dr.object_id.as_deref(), Some("cot-fanout-001"));
            match dr.location_oneof {
                Some(LocationOneof::Location(loc)) => {
                    assert!((loc.y.unwrap() - 51.5074).abs() < 1e-6, "lat mismatch");
                    assert!((loc.x.unwrap() - (-0.1278)).abs() < 1e-6, "lon mismatch");
                    assert!((loc.z.unwrap() - 45.0).abs() < 1e-6, "hae mismatch");
                }
                other => panic!("expected Location, got {other:?}"),
            }
        }
        other => panic!("expected DetectionReport, got {other:?}"),
    }
}

/// Echo-loop prevention: a doc published with origin "sapient" must NOT
/// be fanned out back to the SAPIENT translator (it would loop).
#[tokio::test]
async fn fanout_skips_sapient_origin_echo() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let remote_addr = listener.local_addr().expect("local_addr");

    let backend: Arc<dyn DataSyncBackend> = Arc::new(InMemoryBackend::new_initialized());
    let node = Arc::new(Node::new(backend));
    let translator = Arc::new(SapientTranslator::new());

    let transport = PeatSapientTransport::new(
        SapientRole::Dlmm {
            remote_addr,
            peer_node_id: NodeId::from("hldmm-echo"),
        },
        node.clone(),
        translator.clone(),
    );

    let sink = transport.outbound_sink();
    transport.start().await.expect("start");

    let (mut framed, _) = connection::accept(&listener).await.expect("accept");

    let mgr = Arc::new(TransportManager::new(TransportManagerConfig::default()));
    mgr.register_translator(
        translator.clone(),
        sink,
        TranslatorRegistrationConfig::default(),
    )
    .await
    .expect("register");
    let _handle = mgr
        .start_fanout(node.clone(), vec!["tracks".to_string()])
        .expect("start_fanout");

    // Publish with origin "sapient" — should be filtered out by fan-out.
    let fields = HashMap::from([
        ("lat".to_string(), json!(10.0)),
        ("lon".to_string(), json!(20.0)),
    ]);
    let doc = MeshDocument::with_id("sapient-echo-001".to_string(), fields);
    node.publish_with_origin("tracks", doc, Some("sapient".into()))
        .await
        .expect("publish");

    // Then publish with origin "tak" — this one SHOULD arrive.
    let fields2 = HashMap::from([
        ("lat".to_string(), json!(30.0)),
        ("lon".to_string(), json!(40.0)),
    ]);
    let doc2 = MeshDocument::with_id("tak-sentinel-001".to_string(), fields2);
    node.publish_with_origin("tracks", doc2, Some("tak".into()))
        .await
        .expect("publish sentinel");

    // The sentinel should arrive; the echo should not.
    let mut received = Vec::new();
    for _ in 0..100 {
        match tokio::time::timeout(Duration::from_millis(20), connection::recv(&mut framed)).await {
            Ok(Ok(Some(msg))) => received.push(msg),
            Ok(Ok(None)) => break,
            Ok(Err(err)) => panic!("recv error: {err}"),
            Err(_) => {
                if !received.is_empty() {
                    break;
                }
                continue;
            }
        }
    }

    assert_eq!(received.len(), 1, "only the tak-origin doc should fan out");
    match &received[0].content {
        Some(Content::DetectionReport(dr)) => {
            assert_eq!(dr.object_id.as_deref(), Some("tak-sentinel-001"));
        }
        other => panic!("expected DetectionReport, got {other:?}"),
    }
}

/// Platforms collection is not fanned out (only tracks are subscribed).
#[tokio::test]
async fn fanout_does_not_forward_platforms() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let remote_addr = listener.local_addr().expect("local_addr");

    let backend: Arc<dyn DataSyncBackend> = Arc::new(InMemoryBackend::new_initialized());
    let node = Arc::new(Node::new(backend));
    let translator = Arc::new(SapientTranslator::new());

    let transport = PeatSapientTransport::new(
        SapientRole::Dlmm {
            remote_addr,
            peer_node_id: NodeId::from("hldmm-plat"),
        },
        node.clone(),
        translator.clone(),
    );

    let sink = transport.outbound_sink();
    transport.start().await.expect("start");

    let (mut framed, _) = connection::accept(&listener).await.expect("accept");

    let mgr = Arc::new(TransportManager::new(TransportManagerConfig::default()));
    mgr.register_translator(
        translator.clone(),
        sink,
        TranslatorRegistrationConfig::default(),
    )
    .await
    .expect("register");
    // Only subscribe to "tracks", not "platforms".
    let _handle = mgr
        .start_fanout(node.clone(), vec!["tracks".to_string()])
        .expect("start_fanout");

    // Publish a platforms doc (not subscribed).
    let fields = HashMap::from([
        ("capabilities".to_string(), json!(["radar"])),
        ("operational_status".to_string(), json!("ready")),
    ]);
    let doc = MeshDocument::with_id("node-plat-001".to_string(), fields);
    node.publish_with_origin("platforms", doc, Some("tak".into()))
        .await
        .expect("publish platforms");

    // Follow with a tracks doc as sentinel.
    let fields2 = HashMap::from([
        ("lat".to_string(), json!(55.0)),
        ("lon".to_string(), json!(37.0)),
    ]);
    let doc2 = MeshDocument::with_id("tracks-sentinel".to_string(), fields2);
    node.publish_with_origin("tracks", doc2, Some("tak".into()))
        .await
        .expect("publish sentinel");

    let mut received = Vec::new();
    for _ in 0..100 {
        match tokio::time::timeout(Duration::from_millis(20), connection::recv(&mut framed)).await {
            Ok(Ok(Some(msg))) => received.push(msg),
            Ok(Ok(None)) => break,
            Ok(Err(err)) => panic!("recv error: {err}"),
            Err(_) => {
                if !received.is_empty() {
                    break;
                }
                continue;
            }
        }
    }

    assert_eq!(
        received.len(),
        1,
        "only the tracks sentinel should arrive (platforms not subscribed)"
    );
    match &received[0].content {
        Some(Content::DetectionReport(dr)) => {
            assert_eq!(dr.object_id.as_deref(), Some("tracks-sentinel"));
        }
        other => panic!("expected DetectionReport, got {other:?}"),
    }
}

/// Multiple tracks docs published in quick succession all arrive at the HLDMM
/// in order (sequential fan-out + bounded channel guarantees FIFO).
#[tokio::test]
async fn fanout_preserves_ordering_under_burst() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let remote_addr = listener.local_addr().expect("local_addr");

    let backend: Arc<dyn DataSyncBackend> = Arc::new(InMemoryBackend::new_initialized());
    let node = Arc::new(Node::new(backend));
    let translator = Arc::new(SapientTranslator::new());

    let transport = PeatSapientTransport::new(
        SapientRole::Dlmm {
            remote_addr,
            peer_node_id: NodeId::from("hldmm-burst"),
        },
        node.clone(),
        translator.clone(),
    );

    let sink = transport.outbound_sink();
    transport.start().await.expect("start");

    let (mut framed, _) = connection::accept(&listener).await.expect("accept");

    let mgr = Arc::new(TransportManager::new(TransportManagerConfig::default()));
    mgr.register_translator(
        translator.clone(),
        sink,
        TranslatorRegistrationConfig::default(),
    )
    .await
    .expect("register");
    let _handle = mgr
        .start_fanout(node.clone(), vec!["tracks".to_string()])
        .expect("start_fanout");

    let count = 10;
    for i in 0..count {
        let fields = HashMap::from([
            ("lat".to_string(), json!(i as f64)),
            ("lon".to_string(), json!(i as f64)),
        ]);
        let doc = MeshDocument::with_id(format!("burst-{i:03}"), fields);
        node.publish_with_origin("tracks", doc, Some("tak".into()))
            .await
            .expect("publish");
    }

    let mut received: Vec<SapientMessage> = Vec::new();
    for _ in 0..200 {
        match tokio::time::timeout(Duration::from_millis(20), connection::recv(&mut framed)).await {
            Ok(Ok(Some(msg))) => {
                received.push(msg);
                if received.len() == count {
                    break;
                }
            }
            Ok(Ok(None)) => break,
            Ok(Err(err)) => panic!("recv error: {err}"),
            Err(_) => continue,
        }
    }

    assert_eq!(received.len(), count, "all {count} docs should arrive");

    let ids: Vec<String> = received
        .iter()
        .filter_map(|m| match &m.content {
            Some(Content::DetectionReport(dr)) => dr.object_id.clone(),
            _ => None,
        })
        .collect();
    for (i, id) in ids.iter().enumerate().take(count) {
        assert_eq!(id, &format!("burst-{i:03}"), "ordering mismatch at {i}");
    }
}

/// Inbound SAPIENT detection (from a real DLMM) does NOT echo back out through
/// fan-out — the publish uses origin "sapient", which the fan-out skips.
#[tokio::test]
async fn inbound_sapient_detection_does_not_echo_via_fanout() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let remote_addr = listener.local_addr().expect("local_addr");

    let backend: Arc<dyn DataSyncBackend> = Arc::new(InMemoryBackend::new_initialized());
    let node = Arc::new(Node::new(backend));
    let translator = Arc::new(SapientTranslator::new());

    let transport = PeatSapientTransport::new(
        SapientRole::Dlmm {
            remote_addr,
            peer_node_id: NodeId::from("hldmm-noecho"),
        },
        node.clone(),
        translator.clone(),
    );

    let sink = transport.outbound_sink();
    transport.start().await.expect("start");

    let (mut framed, _) = connection::accept(&listener).await.expect("accept");

    let mgr = Arc::new(TransportManager::new(TransportManagerConfig::default()));
    mgr.register_translator(translator, sink, TranslatorRegistrationConfig::default())
        .await
        .expect("register");
    let _handle = mgr
        .start_fanout(node.clone(), vec!["tracks".to_string()])
        .expect("start_fanout");

    // Send an inbound DetectionReport from the "HLDMM" side — the transport
    // will decode it, publish to mesh with origin "sapient", and the fan-out
    // must NOT re-encode and send it back.
    use peat_sapient::proto::sapient_msg::bsi_flex_335_v2_0::Location;
    use peat_sapient::proto::DetectionReport;

    let inbound = SapientMessage {
        node_id: Some("sensor-echo-test".into()),
        content: Some(Content::DetectionReport(DetectionReport {
            object_id: Some("det-should-not-echo".into()),
            location_oneof: Some(LocationOneof::Location(Location {
                x: Some(1.0),
                y: Some(2.0),
                coordinate_system: Some(1),
                ..Default::default()
            })),
            ..Default::default()
        })),
        ..Default::default()
    };
    connection::send(&mut framed, inbound).await.expect("send");

    // Wait for the detection to land on the mesh.
    use peat_mesh::sync::Query;
    for _ in 0..50 {
        let docs = node.query("tracks", &Query::All).await.expect("query");
        if !docs.is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // Now try to receive — nothing should come back (echo prevention).
    // Wait long enough that any queued fan-out would have drained.
    tokio::time::sleep(Duration::from_millis(200)).await;
    match tokio::time::timeout(Duration::from_millis(100), connection::recv(&mut framed)).await {
        Ok(Ok(Some(msg))) => {
            panic!(
                "inbound SAPIENT detection echoed back via fan-out: {:?}",
                msg.content
            );
        }
        Ok(Ok(None)) => panic!("connection closed unexpectedly"),
        Ok(Err(_)) | Err(_) => { /* expected: no message, or timeout */ }
    }
}
