//! Phase 1 TDD: round-trip encode/decode for all 9 SapientMessage content variants.
//!
//! These tests verify that prost-generated types survive a serialise → deserialise
//! cycle with all representative fields intact. They run without any network or
//! external process — pure in-process protobuf.

use peat_sapient::proto::{
    sapient_msg::bsi_flex_335_v2_0::{
        Alert, AlertAck, DetectionReport, Error as ProtoError, Registration, RegistrationAck,
        StatusReport, Task, TaskAck,
    },
    Content, SapientMessage,
};
use prost::Message;
use prost_types::Timestamp;

fn ts(seconds: i64) -> Timestamp {
    Timestamp { seconds, nanos: 0 }
}

fn wrap(node_id: &str, content: Content) -> SapientMessage {
    SapientMessage {
        timestamp: Some(ts(1_700_000_000)),
        node_id: Some(node_id.into()),
        destination_id: None,
        content: Some(content),
        additional_information: None,
    }
}

fn roundtrip(msg: SapientMessage) -> SapientMessage {
    let bytes = msg.encode_to_vec();
    SapientMessage::decode(bytes.as_slice()).expect("decode failed")
}

#[test]
fn roundtrip_registration() {
    let msg = wrap(
        "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
        Content::Registration(Registration {
            icd_version: Some("BSI Flex 335 v2.0".into()),
            ..Default::default()
        }),
    );
    assert_eq!(roundtrip(msg.clone()), msg);
}

#[test]
fn roundtrip_registration_ack() {
    let msg = wrap(
        "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
        Content::RegistrationAck(RegistrationAck {
            acceptance: Some(true),
            ..Default::default()
        }),
    );
    assert_eq!(roundtrip(msg.clone()), msg);
}

#[test]
fn roundtrip_status_report() {
    let msg = wrap(
        "cccccccc-cccc-cccc-cccc-cccccccccccc",
        Content::StatusReport(StatusReport {
            ..Default::default()
        }),
    );
    assert_eq!(roundtrip(msg.clone()), msg);
}

#[test]
fn roundtrip_detection_report() {
    let msg = wrap(
        "dddddddd-dddd-dddd-dddd-dddddddddddd",
        Content::DetectionReport(DetectionReport {
            report_id: Some("01HZZZZZZZZZZZZZZZZZZZZZZZZ".into()),
            object_id: Some("01HYYYYYYYYYYYYYYYYYYYYYYYY".into()),
            ..Default::default()
        }),
    );
    assert_eq!(roundtrip(msg.clone()), msg);
}

#[test]
fn roundtrip_task() {
    let msg = wrap(
        "eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee",
        Content::Task(Task {
            task_id: Some("01HXXXXXXXXXXXXXXXXXXXXXXXX".into()),
            ..Default::default()
        }),
    );
    assert_eq!(roundtrip(msg.clone()), msg);
}

#[test]
fn roundtrip_task_ack() {
    let msg = wrap(
        "ffffffff-ffff-ffff-ffff-ffffffffffff",
        Content::TaskAck(TaskAck {
            task_id: Some("01HXXXXXXXXXXXXXXXXXXXXXXXX".into()),
            ..Default::default()
        }),
    );
    assert_eq!(roundtrip(msg.clone()), msg);
}

#[test]
fn roundtrip_alert() {
    let msg = wrap(
        "11111111-1111-1111-1111-111111111111",
        Content::Alert(Alert {
            alert_id: Some("01HWWWWWWWWWWWWWWWWWWWWWWW".into()),
            ..Default::default()
        }),
    );
    assert_eq!(roundtrip(msg.clone()), msg);
}

#[test]
fn roundtrip_alert_ack() {
    let msg = wrap(
        "22222222-2222-2222-2222-222222222222",
        Content::AlertAck(AlertAck {
            alert_id: Some("01HWWWWWWWWWWWWWWWWWWWWWWW".into()),
            ..Default::default()
        }),
    );
    assert_eq!(roundtrip(msg.clone()), msg);
}

#[test]
fn roundtrip_error() {
    let msg = wrap(
        "33333333-3333-3333-3333-333333333333",
        Content::Error(ProtoError {
            // error_message is repeated string (Vec<String>), not optional
            error_message: vec!["something went wrong".into(), "secondary error".into()],
            ..Default::default()
        }),
    );
    assert_eq!(roundtrip(msg.clone()), msg);
}

#[test]
fn required_fields_survive_roundtrip() {
    let node_id = "44444444-4444-4444-4444-444444444444";
    let msg = SapientMessage {
        timestamp: Some(ts(1_700_123_456)),
        node_id: Some(node_id.into()),
        destination_id: Some("55555555-5555-5555-5555-555555555555".into()),
        content: Some(Content::Error(ProtoError {
            error_message: vec!["test".into()],
            ..Default::default()
        })),
        additional_information: Some("extra".into()),
    };
    let decoded = roundtrip(msg);
    assert_eq!(decoded.node_id.as_deref(), Some(node_id));
    assert_eq!(decoded.timestamp.unwrap().seconds, 1_700_123_456);
    assert_eq!(
        decoded.destination_id.as_deref(),
        Some("55555555-5555-5555-5555-555555555555")
    );
    assert_eq!(decoded.additional_information.as_deref(), Some("extra"));
}
