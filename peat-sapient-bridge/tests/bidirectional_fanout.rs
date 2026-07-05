//! Bidirectional fan-out e2e tests — both translators registered with a single
//! TransportManager, validating that documents published with one origin are
//! encoded by the _other_ translator and delivered to its OutboundSink.
//!
//! These tests exercise the full codec chain at the TransportManager level:
//!   TAK→mesh→SAPIENT: publish(origin:"tak") → SapientTranslator::encode → sink
//!   SAPIENT→mesh→TAK: publish(origin:"sapient") → CotTranslator::encode → sink
//!
//! No TCP/UDP — both sides use capturing sinks.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use peat_mesh::sync::types::Document as MeshDocument;
use peat_mesh::sync::{DataSyncBackend, InMemoryBackend};
use peat_mesh::transport::{
    FanoutHandle, TranslationContext, TranslatorRegistrationConfig, TransportManager,
    TransportManagerConfig,
};
use peat_mesh::Node;
use peat_mesh_sapient::SapientTranslator;
use peat_tak::CotTranslator;
use serde_json::json;
use tokio::sync::Mutex;

use peat_mesh::transport::OutboundSink;

type CapturedMessages = Arc<Mutex<Vec<(Vec<u8>, Option<String>)>>>;

struct CaptureSink {
    received: CapturedMessages,
}

impl CaptureSink {
    fn new() -> (Self, CapturedMessages) {
        let received: CapturedMessages = Arc::new(Mutex::new(Vec::new()));
        (
            Self {
                received: received.clone(),
            },
            received,
        )
    }
}

#[async_trait]
impl OutboundSink for CaptureSink {
    async fn send_outbound(&self, bytes: Vec<u8>, ctx: &TranslationContext) -> anyhow::Result<()> {
        self.received
            .lock()
            .await
            .push((bytes, ctx.collection.clone()));
        Ok(())
    }
}

fn make_mesh() -> (Arc<Node>, Arc<dyn DataSyncBackend>) {
    let backend: Arc<dyn DataSyncBackend> = Arc::new(InMemoryBackend::new_initialized());
    let node = Arc::new(Node::new(backend.clone()));
    (node, backend)
}

async fn setup_dual_fanout() -> (
    Arc<Node>,
    Arc<Mutex<Vec<(Vec<u8>, Option<String>)>>>,
    Arc<Mutex<Vec<(Vec<u8>, Option<String>)>>>,
    Arc<TransportManager>,
    FanoutHandle,
) {
    let (node, _backend) = make_mesh();

    let sapient_translator = Arc::new(SapientTranslator::new());
    let (sapient_sink, sapient_captured) = CaptureSink::new();

    let cot_translator = Arc::new(CotTranslator::new());
    let (tak_sink, tak_captured) = CaptureSink::new();

    let mgr = Arc::new(TransportManager::new(TransportManagerConfig::default()));

    mgr.register_translator(
        sapient_translator,
        Arc::new(sapient_sink),
        TranslatorRegistrationConfig::default(),
    )
    .await
    .expect("register SAPIENT translator");

    mgr.register_translator(
        cot_translator,
        Arc::new(tak_sink),
        TranslatorRegistrationConfig::default(),
    )
    .await
    .expect("register TAK translator");

    let handle = mgr
        .start_fanout(node.clone(), vec!["tracks".to_string()])
        .expect("start_fanout");

    (node, sapient_captured, tak_captured, mgr, handle)
}

async fn poll_until<T: Send>(
    captured: &Arc<Mutex<Vec<T>>>,
    min_count: usize,
    timeout_ms: u64,
) -> bool {
    let deadline = tokio::time::Instant::now() + Duration::from_millis(timeout_ms);
    loop {
        if captured.lock().await.len() >= min_count {
            return true;
        }
        if tokio::time::Instant::now() > deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

// ---------- TAK → mesh → SAPIENT ----------

/// A CoT-shaped track doc (origin "tak") fans out to SapientTranslator,
/// which encodes it as a SAPIENT DetectionReport protobuf.
#[tokio::test]
async fn tak_origin_doc_reaches_sapient_sink() {
    let (node, sapient_captured, tak_captured, _mgr, _fanout) = setup_dual_fanout().await;

    let fields = HashMap::from([
        ("lat".to_string(), json!(51.5074)),
        ("lon".to_string(), json!(-0.1278)),
        ("hae".to_string(), json!(45.0)),
        ("cot_type".to_string(), json!("a-f-G-U-C")),
        ("callsign".to_string(), json!("BRAVO-1")),
    ]);
    let doc = MeshDocument::with_id("tak-to-sapient-001".to_string(), fields);
    node.publish_with_origin("tracks", doc, Some("tak".into()))
        .await
        .expect("publish");

    assert!(
        poll_until(&sapient_captured, 1, 2000).await,
        "sapient sink should receive the tak-origin doc"
    );

    let captured = sapient_captured.lock().await;
    assert_eq!(captured.len(), 1);
    let (ref bytes, ref collection) = captured[0];
    assert_eq!(collection.as_deref(), Some("tracks"));

    use peat_sapient::proto::{Content, SapientMessage};
    use prost::Message as _;
    let msg = SapientMessage::decode(bytes.as_slice()).expect("decode SAPIENT protobuf");
    match msg.content {
        Some(Content::DetectionReport(dr)) => {
            assert_eq!(dr.object_id.as_deref(), Some("tak-to-sapient-001"));
            use peat_sapient::proto::sapient_msg::bsi_flex_335_v2_0::detection_report::LocationOneof;
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

    // TAK sink must NOT receive the same doc (origin echo prevention).
    let tak = tak_captured.lock().await;
    assert!(
        tak.is_empty(),
        "tak sink should not receive its own origin doc"
    );
}

// ---------- SAPIENT → mesh → TAK ----------

/// A SAPIENT-shaped track doc (origin "sapient") fans out to CotTranslator,
/// which encodes it as CoT XML.
#[tokio::test]
async fn sapient_origin_doc_reaches_tak_sink() {
    let (node, sapient_captured, tak_captured, _mgr, _fanout) = setup_dual_fanout().await;

    let fields = HashMap::from([
        ("lat".to_string(), json!(34.0522)),
        ("lon".to_string(), json!(-118.2437)),
        ("hae".to_string(), json!(0.0)),
        ("sapient_classification".to_string(), json!("vehicle")),
    ]);
    let doc = MeshDocument::with_id("sapient-to-tak-001".to_string(), fields);
    node.publish_with_origin("tracks", doc, Some("sapient".into()))
        .await
        .expect("publish");

    assert!(
        poll_until(&tak_captured, 1, 2000).await,
        "tak sink should receive the sapient-origin doc"
    );

    let captured = tak_captured.lock().await;
    assert_eq!(captured.len(), 1);
    let (ref xml_bytes, ref collection) = captured[0];
    assert_eq!(collection.as_deref(), Some("tracks"));

    let xml = std::str::from_utf8(xml_bytes).expect("CoT XML must be UTF-8");
    assert!(
        xml.contains("uid=\"sapient-to-tak-001\""),
        "CoT XML missing uid: {xml}"
    );
    assert!(
        xml.contains("type=\"a-f-G-U-C\""),
        "CoT XML missing default cot_type: {xml}"
    );
    assert!(xml.contains("lat=\"34.052"), "CoT XML missing lat: {xml}");
    assert!(xml.contains("lon=\"-118.243"), "CoT XML missing lon: {xml}");

    // SAPIENT sink must NOT receive the same doc (origin echo prevention).
    let sapient = sapient_captured.lock().await;
    assert!(
        sapient.is_empty(),
        "sapient sink should not receive its own origin doc"
    );
}

// ---------- Bidirectional burst ----------

/// Interleaved publishes from both origins — each side receives only the
/// _other_ origin's documents, in order, with correct encoding.
#[tokio::test]
async fn bidirectional_interleaved_burst() {
    let (node, sapient_captured, tak_captured, _mgr, _fanout) = setup_dual_fanout().await;

    let n = 5;
    for i in 0..n {
        // TAK → mesh (sapient should receive)
        let fields = HashMap::from([
            ("lat".to_string(), json!(i as f64)),
            ("lon".to_string(), json!(i as f64 + 100.0)),
        ]);
        let doc = MeshDocument::with_id(format!("tak-burst-{i:03}"), fields);
        node.publish_with_origin("tracks", doc, Some("tak".into()))
            .await
            .expect("publish tak");

        // SAPIENT → mesh (tak should receive)
        let fields = HashMap::from([
            ("lat".to_string(), json!(i as f64 + 50.0)),
            ("lon".to_string(), json!(i as f64 + 150.0)),
        ]);
        let doc = MeshDocument::with_id(format!("sapient-burst-{i:03}"), fields);
        node.publish_with_origin("tracks", doc, Some("sapient".into()))
            .await
            .expect("publish sapient");
    }

    assert!(
        poll_until(&sapient_captured, n, 3000).await,
        "sapient should receive {n} docs from tak origin"
    );
    assert!(
        poll_until(&tak_captured, n, 3000).await,
        "tak should receive {n} docs from sapient origin"
    );

    let sapient = sapient_captured.lock().await;
    let tak = tak_captured.lock().await;

    assert_eq!(sapient.len(), n, "sapient sink count");
    assert_eq!(tak.len(), n, "tak sink count");

    // Verify SAPIENT received tak-origin docs as DetectionReports.
    use peat_sapient::proto::{Content, SapientMessage};
    use prost::Message as _;
    for (i, (bytes, _)) in sapient.iter().enumerate() {
        let msg = SapientMessage::decode(bytes.as_slice()).unwrap();
        match msg.content {
            Some(Content::DetectionReport(dr)) => {
                assert_eq!(
                    dr.object_id.as_deref(),
                    Some(format!("tak-burst-{i:03}").as_str()),
                    "sapient ordering mismatch at {i}"
                );
            }
            _ => panic!("expected DetectionReport at position {i}"),
        }
    }

    // Verify TAK received sapient-origin docs as CoT XML.
    for (i, (xml_bytes, _)) in tak.iter().enumerate() {
        let xml = std::str::from_utf8(xml_bytes).unwrap();
        let expected_uid = format!("sapient-burst-{i:03}");
        assert!(
            xml.contains(&format!("uid=\"{expected_uid}\"")),
            "tak ordering mismatch at {i}: {xml}"
        );
    }
}

// ---------- Echo-loop prevention ----------

/// Neither translator receives documents published with its own origin.
#[tokio::test]
async fn echo_loop_prevented_both_directions() {
    let (node, sapient_captured, tak_captured, _mgr, _fanout) = setup_dual_fanout().await;

    // Publish with origin "sapient" — only TAK should get it.
    let fields = HashMap::from([
        ("lat".to_string(), json!(1.0)),
        ("lon".to_string(), json!(2.0)),
    ]);
    let doc = MeshDocument::with_id("echo-test-sapient".to_string(), fields);
    node.publish_with_origin("tracks", doc, Some("sapient".into()))
        .await
        .unwrap();

    // Publish with origin "tak" — only SAPIENT should get it.
    let fields = HashMap::from([
        ("lat".to_string(), json!(3.0)),
        ("lon".to_string(), json!(4.0)),
    ]);
    let doc = MeshDocument::with_id("echo-test-tak".to_string(), fields);
    node.publish_with_origin("tracks", doc, Some("tak".into()))
        .await
        .unwrap();

    // Wait for both sinks to receive exactly 1 message each.
    assert!(poll_until(&sapient_captured, 1, 2000).await);
    assert!(poll_until(&tak_captured, 1, 2000).await);

    // Let any straggler echo arrive.
    tokio::time::sleep(Duration::from_millis(200)).await;

    let sapient = sapient_captured.lock().await;
    let tak = tak_captured.lock().await;

    assert_eq!(
        sapient.len(),
        1,
        "sapient should only get the tak-origin doc"
    );
    assert_eq!(tak.len(), 1, "tak should only get the sapient-origin doc");
}

// ---------- Round-trip fidelity: TAK → mesh → SAPIENT → mesh → TAK ----------

/// Full round-trip: CoT-shaped doc → mesh → SapientTranslator encodes →
/// pretend that's inbound SAPIENT → decode → re-publish → CotTranslator
/// encodes → CoT XML. Lat/lon must survive the double codec hop.
#[tokio::test]
async fn round_trip_tak_sapient_tak_preserves_position() {
    use peat_mesh::transport::Translator;

    let sapient_t = SapientTranslator::new();
    let cot_t = CotTranslator::new();

    let lat = 51.5074_f64;
    let lon = -0.1278_f64;
    let hae = 45.0_f64;

    // Step 1: CoT-shaped mesh doc.
    let fields = HashMap::from([
        ("lat".to_string(), json!(lat)),
        ("lon".to_string(), json!(lon)),
        ("hae".to_string(), json!(hae)),
    ]);
    let original_doc = MeshDocument::with_id("rt-001".to_string(), fields);

    // Step 2: SapientTranslator encodes → protobuf bytes.
    let ctx_out = TranslationContext::outbound().with_collection("tracks");
    let sapient_bytes = sapient_t
        .encode_outbound(&original_doc, &ctx_out)
        .await
        .expect("sapient encode");

    // Step 3: Pretend those bytes arrive inbound on SAPIENT → decode to mesh doc.
    let ctx_in = TranslationContext::inbound("peer-rt");
    let sapient_doc = sapient_t
        .decode_inbound(&sapient_bytes, &ctx_in)
        .await
        .expect("sapient decode")
        .expect("sapient decode should produce doc");

    // Step 4: CotTranslator encodes that mesh doc → CoT XML bytes.
    let cot_bytes = cot_t
        .encode_outbound(&sapient_doc, &ctx_out)
        .await
        .expect("cot encode");

    // Step 5: Verify CoT XML has the right position.
    let xml = std::str::from_utf8(&cot_bytes).unwrap();
    assert!(xml.contains("uid=\"rt-001\""), "uid lost: {xml}");

    // Parse the XML back to verify exact coordinates.
    use peat_protocol::cot::CotEvent;
    let event = CotEvent::from_xml(xml).expect("CoT XML parse");
    assert!(
        (event.point.lat - lat).abs() < 1e-4,
        "lat drift: {} vs {lat}",
        event.point.lat
    );
    assert!(
        (event.point.lon - lon).abs() < 1e-4,
        "lon drift: {} vs {lon}",
        event.point.lon
    );
}
