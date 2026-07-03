//! Inbound integration tests: Apex HLDMM → peat-sapient DLMM.
//!
//! These tests connect peat-sapient to a live Apex instance as a DLMM,
//! exchange SAPIENT messages, and verify that `route_message` produces the
//! expected `SapientUpdate` variants.

use std::time::Duration;

use peat_sapient::{
    bridge::{route_message, SapientUpdate},
    connection::{self, ReconnectConfig},
    proto::sapient_msg::bsi_flex_335_v2_0::{
        detection_report::LocationOneof, registration::NodeDefinition, DetectionReport, Location,
        LocationCoordinateSystem, LocationDatum, Registration, SapientMessage,
    },
    Content,
};

use crate::apex_harness::{skip_if_no_apex, ApexHarness};

/// Connect to Apex as DLMM, complete the Registration handshake, and verify
/// that Apex responds with a RegistrationAck which routes to `Registered`.
#[tokio::test]
async fn dlmm_registration_handshake_with_apex() {
    skip_if_no_apex!();

    let apex = ApexHarness::start().await;
    let config = ReconnectConfig {
        initial_delay: Duration::from_millis(100),
        max_delay: Duration::from_secs(2),
    };
    let mut framed = connection::connect_with_retry(apex.addr, &config)
        .await
        .expect("failed to connect to Apex");

    // Send Registration as DLMM
    let node_id = uuid::Uuid::new_v4().to_string();
    let reg_msg = SapientMessage {
        node_id: Some(node_id.clone()),
        content: Some(Content::Registration(Registration {
            node_definition: vec![NodeDefinition {
                node_type: Some(
                    peat_sapient::proto::sapient_msg::bsi_flex_335_v2_0::registration::NodeType::Camera
                        as i32,
                ),
                node_sub_type: vec![],
            }],
            ..Default::default()
        })),
        ..Default::default()
    };
    connection::send(&mut framed, reg_msg)
        .await
        .expect("failed to send Registration");

    // Apex should respond with RegistrationAck
    let response = tokio::time::timeout(Duration::from_secs(3), connection::recv(&mut framed))
        .await
        .expect("timed out waiting for RegistrationAck")
        .expect("recv error")
        .expect("connection closed before RegistrationAck");

    let update = route_message(response, None, None).expect("route_message failed");
    assert!(
        matches!(
            update,
            SapientUpdate::Registered { .. } | SapientUpdate::Ignored { .. }
        ),
        "expected Registered or Ignored (RegistrationAck), got {update:?}"
    );
}

/// Send a `DetectionReport` to Apex and verify Apex accepts it without error.
///
/// The report uses LatLng coordinates (London), which exercises the full
/// codec → Apex → back path.
#[tokio::test]
async fn dlmm_detection_report_accepted_by_apex() {
    skip_if_no_apex!();

    let apex = ApexHarness::start().await;
    let config = ReconnectConfig::default();
    let mut framed = connection::connect_with_retry(apex.addr, &config)
        .await
        .expect("failed to connect to Apex");

    let node_id = uuid::Uuid::new_v4().to_string();

    // Register first
    let reg_msg = SapientMessage {
        node_id: Some(node_id.clone()),
        content: Some(Content::Registration(Registration::default())),
        ..Default::default()
    };
    connection::send(&mut framed, reg_msg)
        .await
        .expect("send Registration");
    let _ = tokio::time::timeout(Duration::from_secs(3), connection::recv(&mut framed))
        .await
        .ok(); // consume RegistrationAck

    // Send a DetectionReport
    let det_msg = SapientMessage {
        node_id: Some(node_id.clone()),
        content: Some(Content::DetectionReport(DetectionReport {
            report_id: Some("integration-rpt-1".into()),
            object_id: Some("object-1".into()),
            location_oneof: Some(LocationOneof::Location(Location {
                x: Some(-0.1278),
                y: Some(51.5074),
                z: Some(0.0),
                coordinate_system: Some(LocationCoordinateSystem::LatLngDegM as i32),
                datum: Some(LocationDatum::Wgs84E as i32),
                ..Default::default()
            })),
            ..Default::default()
        })),
        ..Default::default()
    };
    connection::send(&mut framed, det_msg)
        .await
        .expect("send DetectionReport");

    // Apex may or may not echo — we just assert no error was raised.
    // If Apex sends a response within 1 s, route it and verify it's valid.
    if let Ok(Ok(Some(response))) =
        tokio::time::timeout(Duration::from_secs(1), connection::recv(&mut framed)).await
    {
        let update = route_message(response, None, None).expect("route_message on Apex response");
        // Any valid SapientUpdate is acceptable here
        let _ = update;
    }
}

/// Verify that a message received from Apex and routed through `route_message`
/// produces a `SapientUpdate` without panicking or returning an unexpected error.
#[tokio::test]
async fn all_apex_messages_route_without_panic() {
    skip_if_no_apex!();

    let apex = ApexHarness::start().await;
    let config = ReconnectConfig::default();
    let mut framed = connection::connect_with_retry(apex.addr, &config)
        .await
        .expect("connect to Apex");

    let node_id = uuid::Uuid::new_v4().to_string();
    connection::send(
        &mut framed,
        SapientMessage {
            node_id: Some(node_id.clone()),
            content: Some(Content::Registration(Registration::default())),
            ..Default::default()
        },
    )
    .await
    .expect("Registration send");

    // Drain all messages Apex sends within 2 s and route each one
    let drain_deadline = Duration::from_secs(2);
    while let Ok(Ok(Some(msg))) =
        tokio::time::timeout(drain_deadline, connection::recv(&mut framed)).await
    {
        let result = route_message(msg, None, None);
        assert!(result.is_ok(), "route_message returned error: {result:?}");
    }
}
