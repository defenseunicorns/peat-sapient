use criterion::{black_box, criterion_group, criterion_main, Criterion};
use peat_mesh::sync::types::Document as MeshDocument;
use peat_mesh::transport::{TranslationContext, Translator};
use peat_mesh_sapient::SapientTranslator;
use peat_sapient::mesh_fields::{platform_to_fields, track_to_fields};
use peat_sapient::proto::sapient_msg::bsi_flex_335_v2_0::detection_report::{
    DetectionReportClassification, LocationOneof,
};
use peat_sapient::proto::sapient_msg::bsi_flex_335_v2_0::location_or_range_bearing::FovOneof;
use peat_sapient::proto::sapient_msg::bsi_flex_335_v2_0::registration::{NodeDefinition, NodeType};
use peat_sapient::proto::sapient_msg::bsi_flex_335_v2_0::status_report::{Power, System};
use peat_sapient::proto::sapient_msg::bsi_flex_335_v2_0::{
    Location, LocationCoordinateSystem, LocationDatum, LocationList, LocationOrRangeBearing,
};
use peat_sapient::proto::{Content, DetectionReport, Registration, SapientMessage, StatusReport};
use peat_sapient::transform::{detection, registration, status};
use peat_schema::capability::v1::{
    Capability, CapabilityAdvertisement, CapabilityType, OperationalStatus,
};
use peat_schema::common::v1::{Position, Timestamp};
use peat_schema::node::v1::{HealthStatus, NodeState, Phase};
use peat_schema::track::v1::{SourceType, Track, TrackPosition, TrackSource, TrackState};
use prost::Message as _;
use std::collections::HashMap;

// ── Fixture builders ────────────────────────────────────────────────────────

fn sample_track() -> Track {
    Track {
        track_id: "trk-bench-001".into(),
        classification: "vehicle".into(),
        confidence: 0.92,
        position: Some(TrackPosition {
            latitude: 34.05,
            longitude: -118.25,
            altitude: 120.0,
            cep_m: 5.0,
            vertical_error_m: 2.0,
        }),
        velocity: None,
        state: TrackState::Confirmed as i32,
        source: Some(TrackSource {
            node_id: "sensor-42".into(),
            sensor_id: "report-1".into(),
            model_version: String::new(),
            source_type: SourceType::Sensor as i32,
        }),
        attributes_json: String::new(),
        first_seen: None,
        last_seen: Some(Timestamp {
            seconds: 1_700_000_000,
            nanos: 500_000_000,
        }),
        observation_count: 3,
    }
}

fn sample_capability_advertisement() -> CapabilityAdvertisement {
    CapabilityAdvertisement {
        node_id: "node-bench-001".into(),
        advertised_at: None,
        capabilities: vec![
            Capability {
                id: "cap-1".into(),
                name: "radar".into(),
                capability_type: CapabilityType::Sensor as i32,
                confidence: 1.0,
                metadata_json: String::new(),
                registered_at: None,
            },
            Capability {
                id: "cap-2".into(),
                name: "camera".into(),
                capability_type: CapabilityType::Sensor as i32,
                confidence: 0.95,
                metadata_json: String::new(),
                registered_at: None,
            },
        ],
        resources: None,
        operational_status: OperationalStatus::Ready as i32,
    }
}

fn sample_node_state() -> NodeState {
    NodeState {
        position: Some(Position {
            latitude: 34.05,
            longitude: -118.25,
            altitude: 50.0,
        }),
        fuel_minutes: 85,
        health: HealthStatus::Nominal as i32,
        phase: Phase::Hierarchy as i32,
        cell_id: None,
        zone_id: None,
        timestamp: None,
    }
}

fn sapient_detection_msg() -> SapientMessage {
    SapientMessage {
        timestamp: None,
        node_id: Some("sensor-bench-001".into()),
        destination_id: None,
        content: Some(Content::DetectionReport(DetectionReport {
            report_id: Some("01HZZZZZZZZZZZZZZZZZZZZZZZZ".into()),
            object_id: Some("det-bench-001".into()),
            location_oneof: Some(LocationOneof::Location(Location {
                x: Some(-118.25),
                y: Some(34.05),
                z: Some(120.0),
                coordinate_system: Some(LocationCoordinateSystem::LatLngDegM as i32),
                datum: Some(LocationDatum::Wgs84E as i32),
                ..Default::default()
            })),
            classification: vec![DetectionReportClassification {
                r#type: Some("vehicle".into()),
                confidence: Some(0.92),
                ..Default::default()
            }],
            ..Default::default()
        })),
        additional_information: None,
    }
}

fn sapient_registration_msg() -> SapientMessage {
    SapientMessage {
        timestamp: None,
        node_id: Some("node-bench-001".into()),
        destination_id: None,
        content: Some(Content::Registration(Registration {
            icd_version: Some("BSI Flex 335 v2.0".into()),
            name: Some("Radar-Alpha".into()),
            node_definition: vec![NodeDefinition {
                node_type: Some(NodeType::Radar as i32),
                node_sub_type: vec!["ground-radar".into()],
            }],
            ..Default::default()
        })),
        additional_information: None,
    }
}

fn sapient_status_msg() -> SapientMessage {
    SapientMessage {
        timestamp: None,
        node_id: Some("node-bench-001".into()),
        destination_id: None,
        content: Some(Content::StatusReport(StatusReport {
            system: Some(System::Ok as i32),
            node_location: Some(Location {
                x: Some(-118.25),
                y: Some(34.05),
                z: Some(50.0),
                coordinate_system: Some(LocationCoordinateSystem::LatLngDegM as i32),
                datum: Some(LocationDatum::Wgs84E as i32),
                ..Default::default()
            }),
            field_of_view: Some(LocationOrRangeBearing {
                fov_oneof: Some(FovOneof::LocationList(LocationList { locations: vec![] })),
            }),
            power: Some(Power {
                level: Some(85),
                ..Default::default()
            }),
            ..Default::default()
        })),
        additional_information: None,
    }
}

// ── mesh_fields projection benchmarks ───────────────────────────────────────

fn bench_track_to_fields(c: &mut Criterion) {
    let track = sample_track();
    c.bench_function("mesh_fields::track_to_fields", |b| {
        b.iter(|| track_to_fields(black_box(&track)))
    });
}

fn bench_platform_to_fields_full(c: &mut Criterion) {
    let adv = sample_capability_advertisement();
    let state = sample_node_state();
    c.bench_function("mesh_fields::platform_to_fields (with state)", |b| {
        b.iter(|| platform_to_fields(black_box(&adv), black_box(Some(&state))))
    });
}

fn bench_platform_to_fields_no_state(c: &mut Criterion) {
    let adv = sample_capability_advertisement();
    c.bench_function("mesh_fields::platform_to_fields (no state)", |b| {
        b.iter(|| platform_to_fields(black_box(&adv), black_box(None)))
    });
}

// ── transform + projection pipeline benchmarks ──────────────────────────────

fn bench_detection_pipeline(c: &mut Criterion) {
    let msg = sapient_detection_msg();
    let dr = match &msg.content {
        Some(Content::DetectionReport(dr)) => dr,
        _ => unreachable!(),
    };
    let node_id = msg.node_id.as_deref().unwrap();
    c.bench_function("pipeline: DetectionReport → Track → fields", |b| {
        b.iter(|| {
            let track =
                detection::from_detection_report(black_box(node_id), None, black_box(dr)).unwrap();
            track_to_fields(black_box(&track))
        })
    });
}

fn bench_registration_pipeline(c: &mut Criterion) {
    let msg = sapient_registration_msg();
    let reg = match &msg.content {
        Some(Content::Registration(r)) => r,
        _ => unreachable!(),
    };
    let node_id = msg.node_id.as_deref().unwrap();
    c.bench_function(
        "pipeline: Registration → CapabilityAdvertisement → fields",
        |b| {
            b.iter(|| {
                let adv = registration::from_registration(black_box(node_id), black_box(reg));
                platform_to_fields(black_box(&adv), None)
            })
        },
    );
}

fn bench_status_pipeline(c: &mut Criterion) {
    let msg = sapient_status_msg();
    let sr = match &msg.content {
        Some(Content::StatusReport(sr)) => sr,
        _ => unreachable!(),
    };
    let node_id = msg.node_id.as_deref().unwrap();
    c.bench_function("pipeline: StatusReport → NodeState → fields", |b| {
        b.iter(|| {
            let (state, cap_delta) = status::from_status_report(black_box(node_id), black_box(sr));
            let adv = cap_delta.unwrap_or(CapabilityAdvertisement {
                node_id: node_id.to_string(),
                ..Default::default()
            });
            platform_to_fields(black_box(&adv), Some(&state))
        })
    });
}

// ── Translator trait benchmarks (decode_inbound / encode_outbound) ───────────

fn bench_decode_detection(c: &mut Criterion) {
    let bytes = sapient_detection_msg().encode_to_vec();
    let translator = SapientTranslator::new();
    let ctx = TranslationContext::inbound("peer-bench");
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    c.bench_function("Translator::decode_inbound (DetectionReport)", |b| {
        b.iter(|| rt.block_on(translator.decode_inbound(black_box(&bytes), &ctx)))
    });
}

fn bench_decode_registration(c: &mut Criterion) {
    let bytes = sapient_registration_msg().encode_to_vec();
    let translator = SapientTranslator::new();
    let ctx = TranslationContext::inbound("peer-bench");
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    c.bench_function("Translator::decode_inbound (Registration)", |b| {
        b.iter(|| rt.block_on(translator.decode_inbound(black_box(&bytes), &ctx)))
    });
}

fn bench_decode_status(c: &mut Criterion) {
    let bytes = sapient_status_msg().encode_to_vec();
    let translator = SapientTranslator::new();
    let ctx = TranslationContext::inbound("peer-bench");
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    c.bench_function("Translator::decode_inbound (StatusReport)", |b| {
        b.iter(|| rt.block_on(translator.decode_inbound(black_box(&bytes), &ctx)))
    });
}

fn bench_encode_outbound(c: &mut Criterion) {
    let doc = MeshDocument::with_id("trk-bench-001".to_string(), HashMap::new());
    let translator = SapientTranslator::new();
    let ctx = TranslationContext::outbound().with_collection("tracks");
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    c.bench_function("Translator::encode_outbound (always None)", |b| {
        b.iter(|| rt.block_on(translator.encode_outbound(black_box(&doc), &ctx)))
    });
}

criterion_group!(
    projection,
    bench_track_to_fields,
    bench_platform_to_fields_full,
    bench_platform_to_fields_no_state,
);

criterion_group!(
    pipeline,
    bench_detection_pipeline,
    bench_registration_pipeline,
    bench_status_pipeline,
);

criterion_group!(
    translator,
    bench_decode_detection,
    bench_decode_registration,
    bench_decode_status,
    bench_encode_outbound,
);

criterion_main!(projection, pipeline, translator);
