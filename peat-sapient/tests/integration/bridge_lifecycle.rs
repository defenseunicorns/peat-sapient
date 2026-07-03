//! Loopback integration tests for `SapientBridge::start()` and `send_task()`.
//!
//! These tests spin up the bridge and a simulated DLMM client in-process.
//! No external tooling (Apex) is required.

use std::time::Duration;

use peat_sapient::{
    bridge::{BridgeConfig, SapientBridge, SapientUpdate},
    connection,
    proto::sapient_msg::bsi_flex_335_v2_0::{
        registration::{NodeDefinition, NodeType},
        sapient_message::Content,
        task_ack, Registration, SapientMessage, TaskAck,
    },
    transform::task::to_task,
};
use peat_schema::command::v1::{
    hierarchical_command::CommandType, mission_order::MissionType, CommandTarget,
    HierarchicalCommand, MissionOrder,
};

fn free_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().port()
}

fn bridge_config(port: u16) -> BridgeConfig {
    BridgeConfig {
        node_id: "hldmm-test-uuid".into(),
        addr: format!("127.0.0.1:{port}").parse().unwrap(),
        detection_rate_limit: None,
        heartbeat_interval: Duration::from_secs(30),
        task_queue_depth: 8,
        task_ttl: Duration::from_secs(60),
    }
}

fn isr_command() -> HierarchicalCommand {
    HierarchicalCommand {
        command_id: "cmd-test-001".into(),
        originator_id: "hldmm-test-uuid".into(),
        target: Some(CommandTarget {
            scope: 1,
            target_ids: vec!["dlmm-test-uuid".into()],
        }),
        command_type: Some(CommandType::MissionOrder(MissionOrder {
            mission_type: MissionType::Isr as i32,
            mission_id: "mission-test".into(),
            description: "test ISR sweep".into(),
            ..Default::default()
        })),
        ..Default::default()
    }
}

fn registration_msg(node_id: &str) -> SapientMessage {
    SapientMessage {
        node_id: Some(node_id.to_string()),
        content: Some(Content::Registration(Registration {
            node_definition: vec![NodeDefinition {
                node_type: Some(NodeType::Camera as i32),
                node_sub_type: vec![],
            }],
            ..Default::default()
        })),
        ..Default::default()
    }
}

/// Start the bridge, connect a DLMM, register it, and verify the bridge emits
/// `Registered` and the DLMM receives a `RegistrationAck`.
#[tokio::test]
async fn bridge_accepts_connection_and_sends_registration_ack() {
    let port = free_port();
    let (bridge, mut updates) = SapientBridge::new(bridge_config(port));
    bridge.start().await.unwrap();

    // Give the listener task a tick to start.
    tokio::task::yield_now().await;

    let addr = format!("127.0.0.1:{port}").parse().unwrap();
    let mut dlmm = connection::connect_with_retry(addr, &connection::ReconnectConfig::default())
        .await
        .unwrap();

    // DLMM sends Registration.
    connection::send(&mut dlmm, registration_msg("dlmm-test-uuid"))
        .await
        .unwrap();

    // DLMM should receive RegistrationAck.
    let ack_msg = connection::recv(&mut dlmm)
        .await
        .unwrap()
        .expect("expected RegistrationAck");
    assert!(
        matches!(ack_msg.content, Some(Content::RegistrationAck(_))),
        "first message back should be RegistrationAck"
    );

    // Bridge should emit Registered.
    let update = tokio::time::timeout(Duration::from_secs(2), updates.recv())
        .await
        .expect("timeout")
        .expect("channel closed");
    assert!(
        matches!(update, SapientUpdate::Registered { ref node_id, .. } if node_id == "dlmm-test-uuid"),
        "expected Registered for dlmm-test-uuid, got {update:?}"
    );
}

/// `send_task` delivers a `Task` to a connected DLMM and the DLMM can read it.
#[tokio::test]
async fn bridge_delivers_task_to_connected_dlmm() {
    let port = free_port();
    let (bridge, mut updates) = SapientBridge::new(bridge_config(port));
    bridge.start().await.unwrap();
    tokio::task::yield_now().await;

    let addr = format!("127.0.0.1:{port}").parse().unwrap();
    let mut dlmm = connection::connect_with_retry(addr, &connection::ReconnectConfig::default())
        .await
        .unwrap();

    // DLMM registers.
    connection::send(&mut dlmm, registration_msg("dlmm-test-uuid"))
        .await
        .unwrap();
    // Consume RegistrationAck.
    connection::recv(&mut dlmm).await.unwrap();

    // Wait for the bridge to process Registration.
    tokio::time::timeout(Duration::from_secs(2), updates.recv())
        .await
        .unwrap();

    // HLDMM sends a task.
    let task_msg = to_task("hldmm-test-uuid", "dlmm-test-uuid", &isr_command()).unwrap();
    bridge.send_task("dlmm-test-uuid", task_msg).await.unwrap();

    // DLMM should receive the Task.
    let received = tokio::time::timeout(Duration::from_secs(2), connection::recv(&mut dlmm))
        .await
        .expect("timeout waiting for Task")
        .unwrap()
        .expect("connection closed");

    assert!(
        matches!(received.content, Some(Content::Task(_))),
        "DLMM should receive Task, got {:?}",
        received.content
    );
}

/// Tasks enqueued before the DLMM connects are replayed when it registers.
#[tokio::test]
async fn bridge_replays_queued_task_on_dlmm_connect() {
    let port = free_port();
    let (bridge, mut updates) = SapientBridge::new(bridge_config(port));
    bridge.start().await.unwrap();
    tokio::task::yield_now().await;

    // Enqueue a task BEFORE the DLMM connects.
    let task_msg = to_task("hldmm-test-uuid", "dlmm-test-uuid", &isr_command()).unwrap();
    bridge.send_task("dlmm-test-uuid", task_msg).await.unwrap();

    // Now the DLMM connects and registers.
    let addr = format!("127.0.0.1:{port}").parse().unwrap();
    let mut dlmm = connection::connect_with_retry(addr, &connection::ReconnectConfig::default())
        .await
        .unwrap();
    connection::send(&mut dlmm, registration_msg("dlmm-test-uuid"))
        .await
        .unwrap();
    // Consume RegistrationAck.
    connection::recv(&mut dlmm).await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), updates.recv())
        .await
        .unwrap();

    // The queued task should be replayed immediately after RegistrationAck.
    let received = tokio::time::timeout(Duration::from_secs(2), connection::recv(&mut dlmm))
        .await
        .expect("timeout waiting for replayed Task")
        .unwrap()
        .expect("connection closed");

    assert!(
        matches!(received.content, Some(Content::Task(_))),
        "DLMM should receive replayed Task, got {:?}",
        received.content
    );
}

/// After `TaskAck::Accepted`, a disconnect + reconnect does NOT replay the task.
#[tokio::test]
async fn task_ack_prevents_replay_on_reconnect() {
    let port = free_port();
    let (bridge, mut updates) = SapientBridge::new(bridge_config(port));
    bridge.start().await.unwrap();
    tokio::task::yield_now().await;

    let addr: std::net::SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();

    // --- First connection: register, receive task, send TaskAck ---
    {
        let mut dlmm =
            connection::connect_with_retry(addr, &connection::ReconnectConfig::default())
                .await
                .unwrap();

        connection::send(&mut dlmm, registration_msg("dlmm-test-uuid"))
            .await
            .unwrap();
        connection::recv(&mut dlmm).await.unwrap(); // RegistrationAck
        tokio::time::timeout(Duration::from_secs(2), updates.recv())
            .await
            .unwrap(); // Registered

        // HLDMM sends task.
        let task_msg = to_task("hldmm-test-uuid", "dlmm-test-uuid", &isr_command()).unwrap();
        let task_id = match &task_msg.content {
            Some(Content::Task(t)) => t.task_id.clone().unwrap(),
            _ => panic!("expected Task content"),
        };
        bridge.send_task("dlmm-test-uuid", task_msg).await.unwrap();

        // DLMM receives Task.
        let received = tokio::time::timeout(Duration::from_secs(2), connection::recv(&mut dlmm))
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(matches!(received.content, Some(Content::Task(_))));

        // DLMM sends TaskAck::Accepted.
        let ack = SapientMessage {
            node_id: Some("dlmm-test-uuid".into()),
            content: Some(Content::TaskAck(TaskAck {
                task_id: Some(task_id),
                task_status: Some(task_ack::TaskStatus::Accepted as i32),
                ..Default::default()
            })),
            ..Default::default()
        };
        connection::send(&mut dlmm, ack).await.unwrap();

        // Wait for the bridge to process the TaskAck.
        tokio::time::timeout(Duration::from_secs(2), updates.recv())
            .await
            .unwrap(); // TaskAcknowledged
    }
    // First connection drops here (dlmm goes out of scope).

    // Wait for NodeDisconnected to be processed.
    tokio::time::timeout(Duration::from_secs(2), updates.recv())
        .await
        .unwrap(); // NodeDisconnected

    // --- Second connection: no task should be replayed ---
    let mut dlmm2 = connection::connect_with_retry(addr, &connection::ReconnectConfig::default())
        .await
        .unwrap();
    connection::send(&mut dlmm2, registration_msg("dlmm-test-uuid"))
        .await
        .unwrap();
    connection::recv(&mut dlmm2).await.unwrap(); // RegistrationAck

    // There should be NO task replayed — queue was cleared by TaskAck.
    let timeout_result =
        tokio::time::timeout(Duration::from_millis(300), connection::recv(&mut dlmm2)).await;
    assert!(
        timeout_result.is_err(),
        "no task should be replayed after TaskAck, but received a message"
    );
}

/// A task whose TTL expires before reconnect is NOT replayed.
#[tokio::test(start_paused = true)]
async fn expired_task_is_not_replayed_on_reconnect() {
    let port = free_port();
    let mut config = bridge_config(port);
    config.task_ttl = Duration::from_secs(5); // short TTL for this test
    let (bridge, mut updates) = SapientBridge::new(config);
    bridge.start().await.unwrap();
    tokio::task::yield_now().await;

    let addr: std::net::SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();

    // Enqueue task before DLMM connects.
    let task_msg = to_task("hldmm-test-uuid", "dlmm-test-uuid", &isr_command()).unwrap();
    bridge.send_task("dlmm-test-uuid", task_msg).await.unwrap();

    // Advance time past the TTL.
    tokio::time::advance(Duration::from_secs(6)).await;

    // DLMM connects.
    let mut dlmm = connection::connect_with_retry(addr, &connection::ReconnectConfig::default())
        .await
        .unwrap();
    connection::send(&mut dlmm, registration_msg("dlmm-test-uuid"))
        .await
        .unwrap();
    connection::recv(&mut dlmm).await.unwrap(); // RegistrationAck
    tokio::time::timeout(Duration::from_secs(2), updates.recv())
        .await
        .unwrap(); // Registered

    // The expired task should NOT be replayed.
    let timeout_result =
        tokio::time::timeout(Duration::from_millis(300), connection::recv(&mut dlmm)).await;
    assert!(
        timeout_result.is_err(),
        "expired task should not be replayed"
    );
}
