//! End-to-end: a fake DLMM sends raw SAPIENT messages over TCP to a
//! `PeatSapientTransport` running in HLDMM mode, and the resulting
//! documents are queryable from a real (in-memory) `peat_mesh::Node`.
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
use peat_sapient::proto::sapient_msg::bsi_flex_335_v2_0::registration::{NodeDefinition, NodeType};
use peat_sapient::proto::sapient_msg::bsi_flex_335_v2_0::status_report::{Power, System};
use peat_sapient::proto::sapient_msg::bsi_flex_335_v2_0::Location;
use peat_sapient::proto::{Content, DetectionReport, Registration, SapientMessage, StatusReport};

/// Bind to an ephemeral port and hand back the address, per this repo's
/// existing convention (`tests/integration/apex_harness.rs`,
/// `bridge_lifecycle.rs`) — an inherent TOCTOU window exists between
/// releasing the bind and `PeatSapientTransport` claiming it; accepted here
/// for the same reason it's accepted there.
fn free_local_addr() -> SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap()
}

/// Start a transport in HLDMM mode on an ephemeral port, returning the
/// transport, its listen address, and the backing mesh `Node`.
async fn start_hldmm() -> (PeatSapientTransport, SocketAddr, Arc<Node>) {
    let backend: Arc<dyn DataSyncBackend> = Arc::new(InMemoryBackend::new_initialized());
    let node = Arc::new(Node::new(backend));
    let translator = Arc::new(SapientTranslator::new());

    let listen_addr = free_local_addr();
    let transport =
        PeatSapientTransport::new(SapientRole::Hldmm { listen_addr }, node.clone(), translator);
    transport.start().await.expect("start");
    tokio::time::sleep(Duration::from_millis(50)).await; // let the accept loop bind
    (transport, listen_addr, node)
}

/// Poll a mesh collection until at least `expected` documents land, or
/// time out after ~1 s.
async fn poll_collection(
    node: &Node,
    collection: &str,
    expected: usize,
) -> Vec<peat_mesh::sync::types::Document> {
    let mut docs = Vec::new();
    for _ in 0..50 {
        docs = node.query(collection, &Query::All).await.expect("query");
        if docs.len() >= expected {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    docs
}

#[tokio::test]
async fn detection_report_over_tcp_lands_as_tracks_document() {
    let (_transport, listen_addr, node) = start_hldmm().await;

    let mut framed = connection::connect(listen_addr)
        .await
        .expect("dlmm connect");
    connection::send(
        &mut framed,
        SapientMessage {
            node_id: Some("sensor-1".into()),
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
            ..Default::default()
        },
    )
    .await
    .expect("send");

    let landed = poll_collection(&node, "tracks", 1).await;
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

#[tokio::test]
async fn registration_over_tcp_lands_as_platforms_document() {
    let (_transport, listen_addr, node) = start_hldmm().await;

    let mut framed = connection::connect(listen_addr)
        .await
        .expect("dlmm connect");
    connection::send(
        &mut framed,
        SapientMessage {
            node_id: Some("radar-alpha".into()),
            content: Some(Content::Registration(Registration {
                icd_version: Some("BSI Flex 335 v2.0".into()),
                name: Some("Radar-Alpha".into()),
                node_definition: vec![NodeDefinition {
                    node_type: Some(NodeType::Radar as i32),
                    node_sub_type: vec!["ground-radar".into()],
                }],
                ..Default::default()
            })),
            ..Default::default()
        },
    )
    .await
    .expect("send");

    let landed = poll_collection(&node, "platforms", 1).await;
    assert_eq!(
        landed.len(),
        1,
        "expected exactly one platforms document to land"
    );
    assert_eq!(landed[0].id.as_deref(), Some("radar-alpha"));
    let capabilities = landed[0]
        .fields
        .get("capabilities")
        .and_then(|v| v.as_array());
    assert!(
        capabilities.is_some(),
        "platforms document should carry capabilities field"
    );
}

#[tokio::test]
async fn status_report_over_tcp_lands_as_platforms_document() {
    let (_transport, listen_addr, node) = start_hldmm().await;

    let mut framed = connection::connect(listen_addr)
        .await
        .expect("dlmm connect");
    connection::send(
        &mut framed,
        SapientMessage {
            node_id: Some("sensor-status-1".into()),
            content: Some(Content::StatusReport(StatusReport {
                system: Some(System::Ok as i32),
                node_location: Some(Location {
                    x: Some(-0.1278),
                    y: Some(51.5074),
                    z: Some(30.0),
                    coordinate_system: Some(1), // LatLngDegM
                    ..Default::default()
                }),
                power: Some(Power {
                    level: Some(85),
                    ..Default::default()
                }),
                ..Default::default()
            })),
            ..Default::default()
        },
    )
    .await
    .expect("send");

    let landed = poll_collection(&node, "platforms", 1).await;
    assert_eq!(
        landed.len(),
        1,
        "expected exactly one platforms document to land"
    );
    assert_eq!(landed[0].id.as_deref(), Some("sensor-status-1"));
    assert_eq!(
        landed[0].fields.get("lat").and_then(|v| v.as_f64()),
        Some(51.5074)
    );
    assert_eq!(
        landed[0].fields.get("lon").and_then(|v| v.as_f64()),
        Some(-0.1278)
    );
}

/// Full DLMM lifecycle: Registration → StatusReport → DetectionReport, each
/// landing in the correct collection. The StatusReport's position is cached
/// by the translator, which would matter if a subsequent DetectionReport
/// used range/bearing coordinates — here we use absolute WGS84 for
/// simplicity but still verify the three message types coexist correctly
/// on one connection.
#[tokio::test]
async fn full_dlmm_lifecycle_lands_in_correct_collections() {
    let (_transport, listen_addr, node) = start_hldmm().await;

    let mut framed = connection::connect(listen_addr)
        .await
        .expect("dlmm connect");

    let node_id = "lifecycle-sensor";

    // 1. Registration
    connection::send(
        &mut framed,
        SapientMessage {
            node_id: Some(node_id.into()),
            content: Some(Content::Registration(Registration {
                icd_version: Some("BSI Flex 335 v2.0".into()),
                name: Some("Lifecycle-Sensor".into()),
                node_definition: vec![NodeDefinition {
                    node_type: Some(NodeType::Camera as i32),
                    node_sub_type: vec![],
                }],
                ..Default::default()
            })),
            ..Default::default()
        },
    )
    .await
    .expect("send registration");

    // 2. StatusReport (with position — cached for future range/bearing use)
    connection::send(
        &mut framed,
        SapientMessage {
            node_id: Some(node_id.into()),
            content: Some(Content::StatusReport(StatusReport {
                system: Some(System::Ok as i32),
                node_location: Some(Location {
                    x: Some(2.3522),
                    y: Some(48.8566),
                    z: Some(25.0),
                    coordinate_system: Some(1), // LatLngDegM
                    ..Default::default()
                }),
                ..Default::default()
            })),
            ..Default::default()
        },
    )
    .await
    .expect("send status");

    // 3. DetectionReport
    connection::send(
        &mut framed,
        SapientMessage {
            node_id: Some(node_id.into()),
            content: Some(Content::DetectionReport(DetectionReport {
                object_id: Some("det-lifecycle-001".into()),
                location_oneof: Some(LocationOneof::Location(Location {
                    x: Some(2.3530),
                    y: Some(48.8570),
                    z: Some(0.0),
                    coordinate_system: Some(1), // LatLngDegM
                    ..Default::default()
                })),
                ..Default::default()
            })),
            ..Default::default()
        },
    )
    .await
    .expect("send detection");

    // platforms: Registration and StatusReport both map to the same node_id
    // document, so there should be exactly 1 platforms document (the
    // StatusReport updates the Registration's document in-place via
    // peat-mesh's field-level LWW merge).
    let platforms = poll_collection(&node, "platforms", 1).await;
    assert!(
        !platforms.is_empty(),
        "expected at least one platforms document"
    );
    assert_eq!(platforms[0].id.as_deref(), Some(node_id));
    assert_eq!(
        platforms[0].fields.get("lat").and_then(|v| v.as_f64()),
        Some(48.8566),
        "StatusReport position should be reflected in platforms document"
    );

    // tracks: DetectionReport lands as a separate collection.
    let tracks = poll_collection(&node, "tracks", 1).await;
    assert_eq!(
        tracks.len(),
        1,
        "expected exactly one tracks document to land"
    );
    assert_eq!(
        tracks[0].fields.get("lat").and_then(|v| v.as_f64()),
        Some(48.8570)
    );
}
