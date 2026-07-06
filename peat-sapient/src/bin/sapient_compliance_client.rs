//! Minimal DLMM compliance client for BSI Flex 335 v2.0 validation.
//!
//! Connects to an HLDMM (e.g. SapientComplianceRunner in HLDMM mode),
//! exercises the full message exchange using peat-sapient's native codec,
//! and exits with code 0 on success.
//!
//! This validates wire-level interop: peat-sapient's Rust codec producing
//! bytes that the Dstl C# FluentValidation validators accept.

use std::net::SocketAddr;
use std::process::ExitCode;

use prost_types::Timestamp;
use tracing::{error, info};

use peat_sapient::connection;
use peat_sapient::proto::sapient_msg::bsi_flex_335_v2_0::detection_report::LocationOneof;
use peat_sapient::proto::sapient_msg::bsi_flex_335_v2_0::registration::{
    location_type, Capability, ConfigurationData, Duration as SapientDuration, LocationType,
    ModeDefinition, ModeType, NodeDefinition, NodeType, RegionDefinition, RegionType,
    StatusDefinition, TaskDefinition, TimeUnits,
};
use peat_sapient::proto::sapient_msg::bsi_flex_335_v2_0::status_report;
use peat_sapient::proto::sapient_msg::bsi_flex_335_v2_0::task_ack::TaskStatus;
use peat_sapient::proto::sapient_msg::bsi_flex_335_v2_0::{
    Location, LocationCoordinateSystem, LocationDatum,
};
use peat_sapient::proto::{
    Content, DetectionReport, Registration, SapientMessage, StatusReport, TaskAck,
};

fn now() -> Option<Timestamp> {
    let d = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    Some(Timestamp {
        seconds: d.as_secs() as i64,
        nanos: d.subsec_nanos() as i32,
    })
}

fn new_uuid() -> String {
    uuid::Uuid::new_v4().to_string()
}

fn new_ulid() -> String {
    ulid::Ulid::new().to_string()
}

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt().with_env_filter("info").init();

    let addr: SocketAddr = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:12000".into())
        .parse()
        .expect("invalid address");

    info!(%addr, "connecting to HLDMM");

    let mut framed = match connection::connect(addr).await {
        Ok(f) => f,
        Err(e) => {
            error!(%e, "failed to connect");
            return ExitCode::FAILURE;
        }
    };

    let node_id = new_uuid();
    let mut steps_passed = 0u32;
    let mut steps_failed = 0u32;

    let registration = SapientMessage {
        timestamp: now(),
        node_id: Some(node_id.clone()),
        destination_id: None,
        content: Some(Content::Registration(Registration {
            icd_version: Some("BSI Flex 335 v2.0".into()),
            name: Some("peat-sapient compliance client".into()),
            short_name: Some("peat-cc".into()),
            node_definition: vec![NodeDefinition {
                node_type: Some(NodeType::Camera.into()),
                node_sub_type: vec!["EO".into()],
            }],
            capabilities: vec![Capability {
                category: Some("Sensor".into()),
                r#type: Some("Electro-Optical".into()),
                value: Some("1080p".into()),
                units: Some("pixels".into()),
            }],
            status_definition: Some(StatusDefinition {
                status_interval: Some(SapientDuration {
                    units: Some(TimeUnits::Seconds.into()),
                    value: Some(5.0),
                }),
                ..Default::default()
            }),
            mode_definition: vec![ModeDefinition {
                mode_name: Some("Default".into()),
                mode_type: Some(ModeType::Permanent.into()),
                mode_description: Some("Default operating mode".into()),
                settle_time: Some(SapientDuration {
                    units: Some(TimeUnits::Seconds.into()),
                    value: Some(1.0),
                }),
                task: Some(TaskDefinition {
                    concurrent_tasks: Some(1),
                    region_definition: Some(RegionDefinition {
                        region_type: vec![RegionType::AreaOfInterest.into()],
                        region_area: vec![LocationType {
                            coordinates_oneof: Some(
                                location_type::CoordinatesOneof::LocationUnits(
                                    LocationCoordinateSystem::LatLngDegM.into(),
                                ),
                            ),
                            datum_oneof: Some(location_type::DatumOneof::LocationDatum(
                                LocationDatum::Wgs84E.into(),
                            )),
                            ..Default::default()
                        }],
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            }],
            config_data: vec![ConfigurationData {
                manufacturer: "DefenseUnicorns".into(),
                model: "peat-sapient".into(),
                software_version: Some("0.1.0".into()),
                ..Default::default()
            }],
            ..Default::default()
        })),
        additional_information: None,
    };

    if let Err(e) = connection::send(&mut framed, registration).await {
        error!(%e, "failed to send Registration");
        return ExitCode::FAILURE;
    }
    info!("sent Registration");

    match connection::recv(&mut framed).await {
        Ok(Some(msg)) => {
            if let Some(Content::RegistrationAck(ack)) = &msg.content {
                if ack.acceptance == Some(true) {
                    info!("received RegistrationAck (accepted)");
                    steps_passed += 1;
                } else {
                    error!("RegistrationAck rejected");
                    steps_failed += 1;
                }
            } else {
                error!(content = ?msg.content, "expected RegistrationAck");
                steps_failed += 1;
            }
        }
        Ok(None) => {
            error!("connection closed before RegistrationAck");
            return ExitCode::FAILURE;
        }
        Err(e) => {
            error!(%e, "recv error waiting for RegistrationAck");
            return ExitCode::FAILURE;
        }
    }

    let status = SapientMessage {
        timestamp: now(),
        node_id: Some(node_id.clone()),
        destination_id: None,
        content: Some(Content::StatusReport(StatusReport {
            report_id: Some(new_ulid()),
            system: Some(status_report::System::Ok.into()),
            info: Some(status_report::Info::New.into()),
            mode: Some("Default".into()),
            ..Default::default()
        })),
        additional_information: None,
    };

    if let Err(e) = connection::send(&mut framed, status).await {
        error!(%e, "failed to send StatusReport");
        steps_failed += 1;
    } else {
        info!("sent StatusReport");
        steps_passed += 1;
    }

    let detection = SapientMessage {
        timestamp: now(),
        node_id: Some(node_id.clone()),
        destination_id: None,
        content: Some(Content::DetectionReport(DetectionReport {
            report_id: Some(new_ulid()),
            object_id: Some(new_ulid()),
            location_oneof: Some(LocationOneof::Location(Location {
                x: Some(-1.8224),
                y: Some(51.1740),
                z: Some(100.0),
                coordinate_system: Some(LocationCoordinateSystem::LatLngDegM.into()),
                datum: Some(LocationDatum::Wgs84E.into()),
                ..Default::default()
            })),
            detection_confidence: Some(0.95),
            ..Default::default()
        })),
        additional_information: None,
    };

    if let Err(e) = connection::send(&mut framed, detection).await {
        error!(%e, "failed to send DetectionReport");
        steps_failed += 1;
    } else {
        info!("sent DetectionReport");
        steps_passed += 1;
    }

    match tokio::time::timeout(
        std::time::Duration::from_secs(10),
        connection::recv(&mut framed),
    )
    .await
    {
        Ok(Ok(Some(msg))) => {
            if let Some(Content::Task(task)) = &msg.content {
                info!(task_id = ?task.task_id, "received Task");

                let task_ack = SapientMessage {
                    timestamp: now(),
                    node_id: Some(node_id.clone()),
                    destination_id: msg.node_id.clone(),
                    content: Some(Content::TaskAck(TaskAck {
                        task_id: task.task_id.clone(),
                        task_status: Some(TaskStatus::Accepted.into()),
                        reason: vec!["Accepted by peat-sapient compliance client".into()],
                        associated_file: None,
                    })),
                    additional_information: None,
                };

                if let Err(e) = connection::send(&mut framed, task_ack).await {
                    error!(%e, "failed to send TaskAck");
                    steps_failed += 1;
                } else {
                    info!("sent TaskAck");
                    steps_passed += 1;
                }
            } else {
                info!(content = ?msg.content, "received non-Task message (skipping)");
            }
        }
        Ok(Ok(None)) => {
            info!("connection closed (no Task received, skipping)");
        }
        Ok(Err(e)) => {
            error!(%e, "recv error waiting for Task");
            steps_failed += 1;
        }
        Err(_) => {
            info!("no Task received within timeout (optional)");
        }
    }

    info!(
        passed = steps_passed,
        failed = steps_failed,
        "compliance run complete"
    );

    if steps_failed > 0 {
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    }
}
