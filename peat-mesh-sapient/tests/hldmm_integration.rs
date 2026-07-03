//! End-to-end: a fake DLMM sends a raw SAPIENT `DetectionReport` over TCP to
//! a `PeatSapientTransport` running in HLDMM mode, and the resulting
//! `tracks` document is queryable from a real (in-memory) `peat_mesh::Node`.
//!
//! Proves the codec is wired correctly against a real `Node` — not just
//! unit-tested against `decode_inbound` in isolation (see
//! `peat-mesh-sapient/src/translator.rs`'s own unit tests for that).

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use peat_mesh::sync::{DataSyncBackend, InMemoryBackend, Query};
use peat_mesh::transport::MeshTransport;
use peat_mesh::Node;
use peat_mesh_sapient::{PeatSapientTransport, SapientRole, SapientTranslator};
use peat_sapient::connection;
use peat_sapient::proto::sapient_msg::bsi_flex_335_v2_0::detection_report::LocationOneof;
use peat_sapient::proto::sapient_msg::bsi_flex_335_v2_0::Location;
use peat_sapient::proto::{Content, DetectionReport, SapientMessage};

/// Bind to an ephemeral port and hand back the address, per this repo's
/// existing convention (`tests/integration/apex_harness.rs`,
/// `bridge_lifecycle.rs`) — an inherent TOCTOU window exists between
/// releasing the bind and `PeatSapientTransport` claiming it; accepted here
/// for the same reason it's accepted there.
fn free_local_addr() -> SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap()
}

#[tokio::test]
async fn detection_report_over_tcp_lands_as_tracks_document() {
    let backend: Arc<dyn DataSyncBackend> = Arc::new(InMemoryBackend::new_initialized());
    let node = Arc::new(Node::new(backend));
    let translator = Arc::new(SapientTranslator::new());

    let listen_addr = free_local_addr();
    let transport =
        PeatSapientTransport::new(SapientRole::Hldmm { listen_addr }, node.clone(), translator);
    transport.start().await.expect("start");
    tokio::time::sleep(Duration::from_millis(50)).await; // let the accept loop bind

    // Act as a fake DLMM: connect and send one DetectionReport.
    let mut framed = connection::connect(listen_addr)
        .await
        .expect("dlmm connect");
    let msg = SapientMessage {
        timestamp: None,
        node_id: Some("sensor-1".into()),
        destination_id: None,
        content: Some(Content::DetectionReport(DetectionReport {
            object_id: Some("det-xyz".into()),
            location_oneof: Some(LocationOneof::Location(Location {
                x: Some(-118.25),
                y: Some(34.05),
                z: Some(10.0),
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
        Some(34.05)
    );
    assert_eq!(
        landed[0].fields.get("lon").and_then(|v| v.as_f64()),
        Some(-118.25)
    );
}
