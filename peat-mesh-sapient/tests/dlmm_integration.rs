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

use peat_mesh::sync::{DataSyncBackend, InMemoryBackend, Query};
use peat_mesh::transport::{MeshTransport, NodeId};
use peat_mesh::Node;
use peat_mesh_sapient::{PeatSapientTransport, SapientRole, SapientTranslator};
use peat_sapient::connection;
use peat_sapient::proto::sapient_msg::bsi_flex_335_v2_0::detection_report::LocationOneof;
use peat_sapient::proto::sapient_msg::bsi_flex_335_v2_0::Location;
use peat_sapient::proto::{Content, DetectionReport, SapientMessage};

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
