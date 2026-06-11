//! Outbound integration tests: peat-sapient HLDMM → DLMM Task flow.
//!
//! These tests exercise the full `transform::task::to_task` → codec → TCP send path.
//! peat-sapient acts as the HLDMM (listener); the test spins up a lightweight DLMM
//! peer using the same `connection` primitives, simulating a sensor that registers and
//! then receives a Task.
//!
//! When Apex is available, `apex_round_trip_task` additionally routes the Task through
//! the live middleware before asserting the TaskAck.

use std::net::SocketAddr;
use std::time::Duration;

use peat_sapient::{
    bridge::{route_message, SapientUpdate},
    connection::{self, ReconnectConfig},
    proto::sapient_msg::bsi_flex_335_v2_0::{task_ack, Registration, SapientMessage, TaskAck},
    transform::task::to_task,
    Content,
};
use peat_schema::command::v1::{
    hierarchical_command::CommandType, CommandTarget, HierarchicalCommand, MissionOrder,
};
use tokio::net::TcpListener;

use crate::apex_harness::skip_if_no_apex;

/// Build a minimal `HierarchicalCommand` for test purposes.
fn test_command(originator: &str, dest: &str) -> HierarchicalCommand {
    HierarchicalCommand {
        command_id: uuid::Uuid::new_v4().to_string(),
        originator_id: originator.to_string(),
        target: Some(CommandTarget {
            target_ids: vec![dest.to_string()],
            ..Default::default()
        }),
        command_type: Some(CommandType::MissionOrder(MissionOrder::default())),
        ..Default::default()
    }
}

/// Bind a random local port and return the listener + its address.
async fn local_listener() -> (TcpListener, SocketAddr) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    (listener, addr)
}

/// End-to-end task send without Apex: peat-sapient encodes a Task, the simulated
/// DLMM peer receives it over TCP, and responds with a `TaskAck`.
///
/// This test does NOT require `apex.py` — it verifies the codec and transform path
/// using two in-process TCP endpoints.
#[tokio::test]
async fn task_sent_and_task_ack_received_loopback() {
    let (listener, hldmm_addr) = local_listener().await;

    let bridge_node_id = uuid::Uuid::new_v4().to_string();
    let sensor_node_id = uuid::Uuid::new_v4().to_string();

    // DLMM peer: accept the connection, receive a Task, and respond with TaskAck.
    let dlmm_handle = tokio::spawn(async move {
        let (mut dlmm_framed, _) = connection::accept(&listener).await.unwrap();

        // Receive the Task sent by the HLDMM (our bridge)
        let task_msg =
            tokio::time::timeout(Duration::from_secs(3), connection::recv(&mut dlmm_framed))
                .await
                .expect("timed out waiting for Task")
                .unwrap()
                .expect("connection closed");

        // Respond with TaskAck
        let task_id = if let Some(Content::Task(ref t)) = task_msg.content {
            t.task_id.clone().unwrap_or_default()
        } else {
            String::new()
        };
        connection::send(
            &mut dlmm_framed,
            SapientMessage {
                node_id: None,
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

        task_msg // return so the hldmm side can inspect it
    });

    // HLDMM (our bridge): connect to the loopback peer, send a Task
    let config = ReconnectConfig {
        initial_delay: Duration::from_millis(50),
        max_delay: Duration::from_millis(200),
    };
    let mut hldmm_framed = connection::connect_with_retry(hldmm_addr, &config)
        .await
        .expect("HLDMM connect");

    let cmd = test_command(&bridge_node_id, &sensor_node_id);
    let task = to_task(&bridge_node_id, &sensor_node_id, &cmd)
        .expect("to_task should not fail for a valid command");

    connection::send(&mut hldmm_framed, task)
        .await
        .expect("send Task");

    // Receive the TaskAck from the simulated DLMM
    let ack_msg = tokio::time::timeout(Duration::from_secs(3), connection::recv(&mut hldmm_framed))
        .await
        .expect("timed out waiting for TaskAck")
        .unwrap()
        .expect("connection closed before TaskAck");

    // Route the TaskAck — it should produce TaskAcknowledged.
    let update = route_message(ack_msg, None, None).expect("route_message on TaskAck");
    assert!(
        matches!(update, SapientUpdate::TaskAcknowledged { accepted: true, .. }),
        "TaskAck should route to TaskAcknowledged(accepted=true), got {update:?}"
    );

    // Verify the DLMM side received a well-formed Task
    let task_received = dlmm_handle.await.unwrap();
    assert!(
        matches!(task_received.content, Some(Content::Task(_))),
        "DLMM should have received a Task, got {task_received:?}"
    );
}

/// Verify Task destination_id and task_id round-trip correctly through the codec.
#[tokio::test]
async fn task_fields_survive_codec_round_trip() {
    let (listener, hldmm_addr) = local_listener().await;

    let bridge_node_id = uuid::Uuid::new_v4().to_string();
    let sensor_node_id = uuid::Uuid::new_v4().to_string();

    let dlmm_handle = tokio::spawn(async move {
        let (mut framed, _) = connection::accept(&listener).await.unwrap();
        tokio::time::timeout(Duration::from_secs(3), connection::recv(&mut framed))
            .await
            .unwrap()
            .unwrap()
            .unwrap()
    });

    let config = ReconnectConfig {
        initial_delay: Duration::from_millis(50),
        max_delay: Duration::from_millis(200),
    };
    let mut sender = connection::connect_with_retry(hldmm_addr, &config)
        .await
        .unwrap();

    let cmd = test_command(&bridge_node_id, &sensor_node_id);
    let task = to_task(&bridge_node_id, &sensor_node_id, &cmd).unwrap();

    // Capture task_id (on Task) and destination_id (on SapientMessage) before sending
    let sent_task_id = if let Some(Content::Task(ref t)) = task.content {
        t.task_id.clone().unwrap_or_default()
    } else {
        panic!("to_task produced wrong Content variant");
    };
    let sent_dest_id = task.destination_id.clone().unwrap_or_default();

    connection::send(&mut sender, task).await.unwrap();

    let received = dlmm_handle.await.unwrap();
    if let Some(Content::Task(ref t)) = received.content {
        assert_eq!(
            t.task_id.as_deref().unwrap_or(""),
            sent_task_id,
            "task_id must survive codec round-trip"
        );
    } else {
        panic!("received Content is not Task: {received:?}");
    }
    assert_eq!(
        received.destination_id.as_deref().unwrap_or(""),
        sent_dest_id,
        "destination_id must survive codec round-trip"
    );
}

/// When Apex is available: connect as DLMM, send a Registration, then
/// attempt to send a Task to Apex and verify the result is not an error.
///
/// Note: Apex as HLDMM does not normally accept Tasks from DLMMs; this test
/// verifies our encode/send path is error-free and that any Apex response
/// routes cleanly through `route_message`.
#[tokio::test]
async fn apex_round_trip_task() {
    skip_if_no_apex!();

    use crate::apex_harness::ApexHarness;
    let apex = ApexHarness::start().await;
    let bridge_node_id = uuid::Uuid::new_v4().to_string();
    let config = ReconnectConfig::default();
    let mut framed = connection::connect_with_retry(apex.addr, &config)
        .await
        .expect("connect to Apex");

    connection::send(
        &mut framed,
        SapientMessage {
            node_id: Some(bridge_node_id.clone()),
            content: Some(Content::Registration(Registration::default())),
            ..Default::default()
        },
    )
    .await
    .expect("Registration send");
    let _ = tokio::time::timeout(Duration::from_secs(3), connection::recv(&mut framed)).await;

    let cmd = test_command(&bridge_node_id, &uuid::Uuid::new_v4().to_string());
    let task = to_task(&bridge_node_id, &bridge_node_id, &cmd).expect("to_task");
    connection::send(&mut framed, task)
        .await
        .expect("Task send should not fail at the TCP/codec level");

    if let Ok(Ok(Some(resp))) =
        tokio::time::timeout(Duration::from_secs(2), connection::recv(&mut framed)).await
    {
        let update = route_message(resp, None, None).expect("route_message on Apex task response");
        let _ = update;
    }
}
