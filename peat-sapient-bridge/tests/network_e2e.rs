//! Network-level e2e: a mock TAK Server ↔ PeatTakTransport ↔ mesh ↔
//! SapientTranslator, validating both inbound and outbound over real TCP.
//!
//! Inbound: mock TAK Server sends CoT XML → PeatTakTransport decodes →
//!          mesh doc → TransportManager fan-out → SapientTranslator encodes →
//!          capturing sink receives DetectionReport protobuf.
//!
//! Outbound: publish SAPIENT-origin doc → fan-out → CotTranslator encodes →
//!           PeatTakTransport outbound channel → TakServerTransport sends →
//!           mock TAK Server receives CoT XML.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use peat_mesh::sync::types::Document as MeshDocument;
use peat_mesh::sync::{DataSyncBackend, InMemoryBackend};
use peat_mesh::transport::{
    MeshTransport, NodeId, OutboundSink, TranslationContext, TranslatorRegistrationConfig,
    TransportManager, TransportManagerConfig,
};
use peat_mesh::Node;
use peat_mesh_sapient::SapientTranslator;
use peat_tak::{CotTranslator, PeatTakTransport, TakMeshConfig};
use serde_json::json;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpListener;
use tokio::sync::Mutex;

// ---------- TAK wire protocol helpers (ProtobufV1 framing) ----------

const TAK_MAGIC: u8 = 0xBF;

fn encode_varint(mut value: u64, buf: &mut Vec<u8>) {
    while value >= 0x80 {
        buf.push((value as u8 & 0x7F) | 0x80);
        value >>= 7;
    }
    buf.push(value as u8);
}

async fn read_varint(reader: &mut BufReader<OwnedReadHalf>) -> Option<u64> {
    let mut value: u64 = 0;
    let mut shift = 0;
    loop {
        let mut byte = [0u8; 1];
        reader.read_exact(&mut byte).await.ok()?;
        value |= ((byte[0] & 0x7F) as u64) << shift;
        if byte[0] & 0x80 == 0 {
            return Some(value);
        }
        shift += 7;
        if shift > 63 {
            return None;
        }
    }
}

async fn send_tak_frame(writer: &mut OwnedWriteHalf, xml: &str) {
    let payload = xml.as_bytes();
    let mut frame = Vec::with_capacity(1 + 5 + payload.len());
    frame.push(TAK_MAGIC);
    encode_varint(payload.len() as u64, &mut frame);
    frame.extend_from_slice(payload);
    writer.write_all(&frame).await.expect("write tak frame");
}

async fn recv_tak_frame(reader: &mut BufReader<OwnedReadHalf>) -> Option<String> {
    let mut magic = [0u8; 1];
    match reader.read_exact(&mut magic).await {
        Ok(_) if magic[0] == TAK_MAGIC => {}
        _ => return None,
    }
    let length = read_varint(reader).await?;
    if length > 1024 * 1024 {
        return None;
    }
    let mut payload = vec![0u8; length as usize];
    reader.read_exact(&mut payload).await.ok()?;
    String::from_utf8(payload).ok()
}

// ---------- Capturing OutboundSink ----------

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

async fn poll_captured(captured: &CapturedMessages, min_count: usize, timeout_ms: u64) -> bool {
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

// ---------- Inbound: TAK Server → mesh → SAPIENT ----------

/// Mock TAK Server sends a CoT XML event over TCP → PeatTakTransport reads it,
/// decodes via CotTranslator, publishes to mesh → TransportManager fan-out
/// encodes via SapientTranslator → capturing sink receives a DetectionReport.
#[tokio::test]
async fn inbound_cot_from_tak_server_reaches_sapient_sink() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind mock TAK Server");
    let server_addr = listener.local_addr().expect("local_addr");

    let backend: Arc<dyn DataSyncBackend> = Arc::new(InMemoryBackend::new_initialized());
    let node = Arc::new(Node::new(backend));

    let cot_translator = Arc::new(CotTranslator::new());
    let sapient_translator = Arc::new(SapientTranslator::new());

    let tak_config = TakMeshConfig {
        server_addr,
        peer_node_id: NodeId::from("mock-tak-server"),
        use_tls: false,
        identity: None,
        max_message_bytes: None,
    };

    let tak_transport = PeatTakTransport::new(tak_config, node.clone(), cot_translator.clone());
    let tak_sink = tak_transport.outbound_sink();

    // Start the transport (spawns worker that dials the mock server).
    tak_transport.start().await.expect("start PeatTakTransport");

    // Accept the connection from PeatTakTransport's worker.
    let (stream, _) = tokio::time::timeout(Duration::from_secs(5), listener.accept())
        .await
        .expect("accept timeout")
        .expect("accept");
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);

    // Read and discard the presence announcement.
    let presence = recv_tak_frame(&mut reader).await.expect("read presence");
    assert!(
        presence.contains("<event"),
        "presence should be a CoT event: {presence}"
    );

    // Set up fan-out: both translators, capturing sink for SAPIENT.
    let (sapient_sink, sapient_captured) = CaptureSink::new();
    let mgr = Arc::new(TransportManager::new(TransportManagerConfig::default()));
    mgr.register_translator(
        cot_translator,
        tak_sink,
        TranslatorRegistrationConfig::default(),
    )
    .await
    .expect("register CotTranslator");
    mgr.register_translator(
        sapient_translator,
        Arc::new(sapient_sink),
        TranslatorRegistrationConfig::default(),
    )
    .await
    .expect("register SapientTranslator");
    let _fanout = mgr
        .start_fanout(node.clone(), vec!["tracks".to_string()])
        .expect("start_fanout");

    // Mock TAK Server sends a CoT event.
    let cot_xml = r#"<?xml version="1.0" encoding="UTF-8"?><event version="2.0" uid="tank-alpha" type="a-f-G-U-C" time="2026-07-01T00:00:00Z" start="2026-07-01T00:00:00Z" stale="2026-07-01T00:05:00Z" how="m-g"><point lat="51.5074" lon="-0.1278" hae="30.0" ce="9999999.0" le="9999999.0"/><detail><contact callsign="TANK-A"/></detail></event>"#;
    send_tak_frame(&mut write_half, cot_xml).await;

    // Wait for the SAPIENT sink to receive the fan-out encoded message.
    assert!(
        poll_captured(&sapient_captured, 1, 5000).await,
        "sapient sink should receive the inbound CoT event"
    );

    let captured = sapient_captured.lock().await;
    let (ref bytes, _) = captured[0];

    use peat_sapient::proto::sapient_msg::bsi_flex_335_v2_0::detection_report::LocationOneof;
    use peat_sapient::proto::{Content, SapientMessage};
    use prost::Message as _;
    let msg = SapientMessage::decode(bytes.as_slice()).expect("decode protobuf");
    match msg.content {
        Some(Content::DetectionReport(dr)) => {
            assert_eq!(dr.object_id.as_deref(), Some("tank-alpha"));
            match dr.location_oneof {
                Some(LocationOneof::Location(loc)) => {
                    assert!((loc.y.unwrap() - 51.5074).abs() < 1e-4, "lat: {:?}", loc.y);
                    assert!(
                        (loc.x.unwrap() - (-0.1278)).abs() < 1e-4,
                        "lon: {:?}",
                        loc.x
                    );
                }
                other => panic!("expected Location, got {other:?}"),
            }
        }
        other => panic!("expected DetectionReport, got {other:?}"),
    }

    tak_transport.stop().await.ok();
}

// ---------- Outbound: SAPIENT → mesh → TAK Server ----------

/// Publish a SAPIENT-origin tracks doc on the mesh → TransportManager fan-out
/// encodes via CotTranslator → PeatTakTransport outbound channel →
/// TakServerTransport sends framed CoT XML → mock TAK Server receives it.
#[tokio::test]
async fn outbound_sapient_doc_reaches_tak_server_as_cot_xml() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind mock TAK Server");
    let server_addr = listener.local_addr().expect("local_addr");

    let backend: Arc<dyn DataSyncBackend> = Arc::new(InMemoryBackend::new_initialized());
    let node = Arc::new(Node::new(backend));

    let cot_translator = Arc::new(CotTranslator::new());
    let sapient_translator = Arc::new(SapientTranslator::new());

    let tak_config = TakMeshConfig {
        server_addr,
        peer_node_id: NodeId::from("mock-tak-server"),
        use_tls: false,
        identity: None,
        max_message_bytes: None,
    };

    let tak_transport = PeatTakTransport::new(tak_config, node.clone(), cot_translator.clone());
    let tak_sink = tak_transport.outbound_sink();

    tak_transport.start().await.expect("start PeatTakTransport");

    let (stream, _) = tokio::time::timeout(Duration::from_secs(5), listener.accept())
        .await
        .expect("accept timeout")
        .expect("accept");
    let (read_half, _write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);

    // Read and discard presence.
    let _presence = recv_tak_frame(&mut reader).await.expect("read presence");

    // Set up fan-out.
    let (sapient_sink, _sapient_captured) = CaptureSink::new();
    let mgr = Arc::new(TransportManager::new(TransportManagerConfig::default()));
    mgr.register_translator(
        cot_translator,
        tak_sink,
        TranslatorRegistrationConfig::default(),
    )
    .await
    .expect("register CotTranslator");
    mgr.register_translator(
        sapient_translator,
        Arc::new(sapient_sink),
        TranslatorRegistrationConfig::default(),
    )
    .await
    .expect("register SapientTranslator");
    let _fanout = mgr
        .start_fanout(node.clone(), vec!["tracks".to_string()])
        .expect("start_fanout");

    // Publish a SAPIENT-origin tracks doc.
    let fields = HashMap::from([
        ("lat".to_string(), json!(34.0522)),
        ("lon".to_string(), json!(-118.2437)),
        ("hae".to_string(), json!(100.0)),
    ]);
    let doc = MeshDocument::with_id("sapient-outbound-001".to_string(), fields);
    node.publish_with_origin("tracks", doc, Some("sapient".into()))
        .await
        .expect("publish");

    // Mock TAK Server reads the CoT XML that PeatTakTransport sent.
    let xml = tokio::time::timeout(Duration::from_secs(5), recv_tak_frame(&mut reader))
        .await
        .expect("recv timeout")
        .expect("recv tak frame");

    assert!(
        xml.contains("uid=\"sapient-outbound-001\""),
        "CoT XML missing uid: {xml}"
    );
    assert!(xml.contains("lat=\"34.052"), "CoT XML missing lat: {xml}");
    assert!(xml.contains("lon=\"-118.243"), "CoT XML missing lon: {xml}");
    assert!(
        xml.contains("type=\"a-f-G-U-C\""),
        "CoT XML should have default cot_type: {xml}"
    );

    tak_transport.stop().await.ok();
}

// ---------- Bidirectional on the same connection ----------

/// Full round-trip on a single mock TAK Server connection:
/// 1. Mock sends CoT inbound → verifies SAPIENT sink gets DetectionReport
/// 2. Publish SAPIENT-origin doc → verifies mock receives CoT XML
#[tokio::test]
async fn bidirectional_over_single_connection() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let server_addr = listener.local_addr().expect("local_addr");

    let backend: Arc<dyn DataSyncBackend> = Arc::new(InMemoryBackend::new_initialized());
    let node = Arc::new(Node::new(backend));

    let cot_translator = Arc::new(CotTranslator::new());
    let sapient_translator = Arc::new(SapientTranslator::new());

    let tak_config = TakMeshConfig {
        server_addr,
        peer_node_id: NodeId::from("mock-tak"),
        use_tls: false,
        identity: None,
        max_message_bytes: None,
    };

    let tak_transport = PeatTakTransport::new(tak_config, node.clone(), cot_translator.clone());
    let tak_sink = tak_transport.outbound_sink();
    tak_transport.start().await.expect("start");

    let (stream, _) = tokio::time::timeout(Duration::from_secs(5), listener.accept())
        .await
        .expect("accept timeout")
        .expect("accept");
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);
    let _presence = recv_tak_frame(&mut reader).await.expect("presence");

    let (sapient_sink, sapient_captured) = CaptureSink::new();
    let mgr = Arc::new(TransportManager::new(TransportManagerConfig::default()));
    mgr.register_translator(
        cot_translator,
        tak_sink,
        TranslatorRegistrationConfig::default(),
    )
    .await
    .unwrap();
    mgr.register_translator(
        sapient_translator,
        Arc::new(sapient_sink),
        TranslatorRegistrationConfig::default(),
    )
    .await
    .unwrap();
    let _fanout = mgr
        .start_fanout(node.clone(), vec!["tracks".to_string()])
        .unwrap();

    // --- Direction 1: TAK → mesh → SAPIENT ---
    let inbound_xml = r#"<?xml version="1.0" encoding="UTF-8"?><event version="2.0" uid="bidi-in-001" type="a-f-G-U-C" time="2026-07-01T00:00:00Z" start="2026-07-01T00:00:00Z" stale="2026-07-01T00:05:00Z" how="m-g"><point lat="40.7128" lon="-74.006" hae="0.0" ce="9999999.0" le="9999999.0"/><detail/></event>"#;
    send_tak_frame(&mut write_half, inbound_xml).await;

    assert!(
        poll_captured(&sapient_captured, 1, 5000).await,
        "sapient sink should receive inbound CoT"
    );

    {
        let captured = sapient_captured.lock().await;
        use peat_sapient::proto::{Content, SapientMessage};
        use prost::Message as _;
        let msg = SapientMessage::decode(captured[0].0.as_slice()).unwrap();
        match msg.content {
            Some(Content::DetectionReport(dr)) => {
                assert_eq!(dr.object_id.as_deref(), Some("bidi-in-001"));
            }
            other => panic!("expected DetectionReport, got {other:?}"),
        }
    }

    // --- Direction 2: SAPIENT → mesh → TAK ---
    let fields = HashMap::from([
        ("lat".to_string(), json!(48.8566)),
        ("lon".to_string(), json!(2.3522)),
    ]);
    let doc = MeshDocument::with_id("bidi-out-001".to_string(), fields);
    node.publish_with_origin("tracks", doc, Some("sapient".into()))
        .await
        .unwrap();

    let outbound_xml = tokio::time::timeout(Duration::from_secs(5), recv_tak_frame(&mut reader))
        .await
        .expect("recv timeout")
        .expect("recv outbound CoT");

    assert!(
        outbound_xml.contains("uid=\"bidi-out-001\""),
        "outbound CoT missing uid: {outbound_xml}"
    );
    assert!(
        outbound_xml.contains("lat=\"48.856"),
        "outbound CoT missing lat: {outbound_xml}"
    );

    tak_transport.stop().await.ok();
}
