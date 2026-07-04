//! Combined e2e: `SapientBridge` handles the TCP connection (C2 + data),
//! `run_bridge_subscriber` publishes updates to a real `peat_mesh::Node`,
//! proving the two integration surfaces compose correctly.
//!
//! In this topology the bridge is the single HLDMM endpoint — it handles
//! registration, tasking, ack, and detection routing. The subscriber
//! consumes the bridge's `SapientUpdate` stream and projects it into
//! `tracks` and `platforms` collections on the mesh `Node`.

use std::sync::Arc;
use std::time::Duration;

use peat_mesh::sync::{DataSyncBackend, InMemoryBackend, Query};
use peat_mesh::Node;
use peat_mesh_sapient::run_bridge_subscriber;
use peat_sapient::bridge::{BridgeConfig, SapientBridge};
use peat_sapient::connection;
use peat_sapient::proto::sapient_msg::bsi_flex_335_v2_0::detection_report::LocationOneof;
use peat_sapient::proto::sapient_msg::bsi_flex_335_v2_0::registration::{NodeDefinition, NodeType};
use peat_sapient::proto::sapient_msg::bsi_flex_335_v2_0::status_report::{Power, System};
use peat_sapient::proto::sapient_msg::bsi_flex_335_v2_0::{task_ack, Location};
use peat_sapient::proto::{
    Content, DetectionReport, Registration, SapientMessage, StatusReport, TaskAck,
};
use peat_sapient::transform::task::to_task;
use peat_schema::command::v1::{
    hierarchical_command::CommandType, mission_order::MissionType, CommandTarget,
    HierarchicalCommand, MissionOrder,
};

fn bridge_config() -> BridgeConfig {
    BridgeConfig {
        node_id: "hldmm-e2e".into(),
        addr: "127.0.0.1:0".parse().unwrap(),
        detection_rate_limit: None,
        heartbeat_interval: Duration::from_secs(30),
        task_queue_depth: 8,
        task_ttl: Duration::from_secs(60),
    }
}

fn registration_msg(node_id: &str) -> SapientMessage {
    SapientMessage {
        node_id: Some(node_id.into()),
        content: Some(Content::Registration(Registration {
            icd_version: Some("BSI Flex 335 v2.0".into()),
            name: Some("E2E-Sensor".into()),
            node_definition: vec![NodeDefinition {
                node_type: Some(NodeType::Camera as i32),
                node_sub_type: vec![],
            }],
            ..Default::default()
        })),
        ..Default::default()
    }
}

fn status_msg(node_id: &str) -> SapientMessage {
    SapientMessage {
        node_id: Some(node_id.into()),
        content: Some(Content::StatusReport(StatusReport {
            system: Some(System::Ok as i32),
            node_location: Some(Location {
                x: Some(-0.1278),
                y: Some(51.5074),
                z: Some(30.0),
                coordinate_system: Some(1),
                ..Default::default()
            }),
            power: Some(Power {
                level: Some(90),
                ..Default::default()
            }),
            ..Default::default()
        })),
        ..Default::default()
    }
}

fn detection_msg(node_id: &str, object_id: &str) -> SapientMessage {
    SapientMessage {
        node_id: Some(node_id.into()),
        content: Some(Content::DetectionReport(DetectionReport {
            object_id: Some(object_id.into()),
            location_oneof: Some(LocationOneof::Location(Location {
                x: Some(-0.1280),
                y: Some(51.5076),
                z: Some(0.0),
                coordinate_system: Some(1),
                ..Default::default()
            })),
            ..Default::default()
        })),
        ..Default::default()
    }
}

/// Poll a mesh collection until at least `expected` documents land.
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

/// Full DLMM lifecycle through bridge + mesh subscriber:
/// Registration → StatusReport → DetectionReport → Task → TaskAck,
/// with platforms and tracks landing in the mesh Node.
#[tokio::test]
async fn bridge_to_mesh_full_lifecycle() {
    let backend: Arc<dyn DataSyncBackend> = Arc::new(InMemoryBackend::new_initialized());
    let node = Arc::new(Node::new(backend));

    let (bridge, updates) = SapientBridge::new(bridge_config());
    let addr = bridge.start().await.unwrap();

    // Spawn the subscriber — it reads from the bridge's update channel and
    // publishes to the mesh Node.
    tokio::spawn(run_bridge_subscriber(updates, node.clone()));

    // --- DLMM connects ---
    let sensor_id = "e2e-sensor-1";
    let mut dlmm = connection::connect_with_retry(addr, &connection::ReconnectConfig::default())
        .await
        .unwrap();

    // 1. Registration → platforms
    connection::send(&mut dlmm, registration_msg(sensor_id))
        .await
        .unwrap();
    connection::recv(&mut dlmm).await.unwrap(); // RegistrationAck

    let platforms = poll_collection(&node, "platforms", 1).await;
    assert_eq!(
        platforms.len(),
        1,
        "Registration should land as a platforms doc"
    );
    assert_eq!(platforms[0].id.as_deref(), Some(sensor_id));

    // 2. StatusReport → platforms (updates same document with position)
    connection::send(&mut dlmm, status_msg(sensor_id))
        .await
        .unwrap();

    // Poll until the platforms doc has lat (added by StatusReport)
    let mut has_lat = false;
    for _ in 0..50 {
        let docs = node.query("platforms", &Query::All).await.unwrap();
        if docs.first().and_then(|d| d.fields.get("lat")).is_some() {
            has_lat = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(has_lat, "StatusReport should add lat to the platforms doc");

    // 3. DetectionReport → tracks
    connection::send(&mut dlmm, detection_msg(sensor_id, "det-e2e-001"))
        .await
        .unwrap();

    let tracks = poll_collection(&node, "tracks", 1).await;
    assert_eq!(
        tracks.len(),
        1,
        "DetectionReport should land as a tracks doc"
    );
    assert_eq!(
        tracks[0].fields.get("lat").and_then(|v| v.as_f64()),
        Some(51.5076)
    );

    // 4. Task → DLMM receives it
    let cmd = HierarchicalCommand {
        command_id: "cmd-e2e-001".into(),
        originator_id: "hldmm-e2e".into(),
        target: Some(CommandTarget {
            scope: 1,
            target_ids: vec![sensor_id.into()],
        }),
        command_type: Some(CommandType::MissionOrder(MissionOrder {
            mission_type: MissionType::Isr as i32,
            mission_id: "mission-e2e".into(),
            description: "e2e ISR sweep".into(),
            ..Default::default()
        })),
        ..Default::default()
    };
    let task_msg = to_task("hldmm-e2e", sensor_id, &cmd).unwrap();
    let task_id = match &task_msg.content {
        Some(Content::Task(t)) => t.task_id.clone().unwrap(),
        _ => panic!("expected Task content"),
    };
    bridge
        .send_task(sensor_id, task_msg, Some("cmd-e2e-001".into()))
        .await
        .unwrap();

    let received = tokio::time::timeout(Duration::from_secs(2), connection::recv(&mut dlmm))
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(
        matches!(received.content, Some(Content::Task(_))),
        "DLMM should receive Task"
    );

    // 5. TaskAck — bridge processes it (subscriber skips C2 updates, no
    //    mesh document expected).
    connection::send(
        &mut dlmm,
        SapientMessage {
            node_id: Some(sensor_id.into()),
            content: Some(Content::TaskAck(TaskAck {
                task_id: Some(task_id),
                task_status: Some(task_ack::TaskStatus::Accepted as i32),
                ..Default::default()
            })),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    // Give the bridge time to process the ack.
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Final state: 1 platforms doc, 1 tracks doc, task queue drained.
    let final_platforms = node.query("platforms", &Query::All).await.unwrap();
    let final_tracks = node.query("tracks", &Query::All).await.unwrap();
    assert_eq!(final_platforms.len(), 1);
    assert_eq!(final_tracks.len(), 1);
}

/// Multiple detections from one DLMM all land as distinct tracks documents.
#[tokio::test]
async fn bridge_to_mesh_multiple_detections() {
    let backend: Arc<dyn DataSyncBackend> = Arc::new(InMemoryBackend::new_initialized());
    let node = Arc::new(Node::new(backend));

    let (bridge, updates) = SapientBridge::new(bridge_config());
    let addr = bridge.start().await.unwrap();
    tokio::spawn(run_bridge_subscriber(updates, node.clone()));

    let sensor_id = "multi-det-sensor";
    let mut dlmm = connection::connect_with_retry(addr, &connection::ReconnectConfig::default())
        .await
        .unwrap();

    connection::send(&mut dlmm, registration_msg(sensor_id))
        .await
        .unwrap();
    connection::recv(&mut dlmm).await.unwrap(); // RegistrationAck

    // Send 5 distinct detections.
    for i in 0..5 {
        connection::send(
            &mut dlmm,
            detection_msg(sensor_id, &format!("det-multi-{i}")),
        )
        .await
        .unwrap();
    }

    let tracks = poll_collection(&node, "tracks", 5).await;
    assert_eq!(
        tracks.len(),
        5,
        "all 5 detections should land as distinct tracks documents"
    );
}
