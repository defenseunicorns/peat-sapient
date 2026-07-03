use std::time::Duration;

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use peat_sapient::bridge::route_message;
use peat_sapient::proto::sapient_msg::bsi_flex_335_v2_0::alert::{
    AlertStatus, AlertType, DiscretePriority, LocationOneof as AlertLocationOneof,
};
use peat_sapient::proto::sapient_msg::bsi_flex_335_v2_0::alert_ack::AlertAckStatus;
use peat_sapient::proto::sapient_msg::bsi_flex_335_v2_0::task::Control;
use peat_sapient::proto::sapient_msg::bsi_flex_335_v2_0::task_ack::TaskStatus;
use peat_sapient::proto::sapient_msg::bsi_flex_335_v2_0::{
    Alert, AlertAck, AssociatedDetection, Location, LocationCoordinateSystem, LocationDatum, Task,
    TaskAck,
};
use peat_sapient::proto::{Content, SapientMessage};
use peat_sapient::task_queue::TaskQueue;
use peat_sapient::transform::alert;
use peat_sapient::transform::task as task_transform;
use peat_schema::command::v1::{
    hierarchical_command::CommandType, mission_order::MissionType, CommandTarget,
    HierarchicalCommand, MissionOrder,
};

// ── Fixture builders ────────────────────────────────────────────────────────

fn alert_msg() -> SapientMessage {
    SapientMessage {
        timestamp: None,
        node_id: Some("sensor-bench-001".into()),
        destination_id: None,
        content: Some(Content::Alert(Alert {
            alert_id: Some("01HZALERT0000000000000000A".into()),
            alert_type: Some(AlertType::Warning as i32),
            status: Some(AlertStatus::Active as i32),
            priority: Some(DiscretePriority::High as i32),
            description: Some("Perimeter breach detected".into()),
            confidence: Some(0.92),
            ranking: Some(0.85),
            location_oneof: Some(AlertLocationOneof::Location(Location {
                x: Some(-118.25),
                y: Some(34.05),
                z: Some(5.0),
                coordinate_system: Some(LocationCoordinateSystem::LatLngDegM as i32),
                datum: Some(LocationDatum::Wgs84E as i32),
                ..Default::default()
            })),
            associated_detection: vec![
                AssociatedDetection {
                    object_id: Some("det-001".into()),
                    ..Default::default()
                },
                AssociatedDetection {
                    object_id: Some("det-002".into()),
                    ..Default::default()
                },
            ],
            ..Default::default()
        })),
        additional_information: None,
    }
}

fn task_ack_msg(task_id: &str, status: TaskStatus) -> SapientMessage {
    SapientMessage {
        timestamp: None,
        node_id: Some("dlmm-bench-001".into()),
        destination_id: None,
        content: Some(Content::TaskAck(TaskAck {
            task_id: Some(task_id.into()),
            task_status: Some(status as i32),
            reason: vec!["acknowledged".into()],
            ..Default::default()
        })),
        additional_information: None,
    }
}

fn task_sapient_msg(task_id: &str) -> SapientMessage {
    SapientMessage {
        node_id: Some("hldmm-bench".into()),
        destination_id: Some("dlmm-bench".into()),
        content: Some(Content::Task(Task {
            task_id: Some(task_id.to_string()),
            task_name: Some("bench-task".into()),
            control: Some(Control::Start as i32),
            ..Default::default()
        })),
        ..Default::default()
    }
}

fn isr_command() -> HierarchicalCommand {
    HierarchicalCommand {
        command_id: "cmd-bench-001".into(),
        originator_id: "origin-node".into(),
        target: Some(CommandTarget {
            scope: 1,
            target_ids: vec!["target-node-uuid".into()],
        }),
        command_type: Some(CommandType::MissionOrder(MissionOrder {
            mission_type: MissionType::Isr as i32,
            mission_id: "mission-bench".into(),
            description: "ISR sweep".into(),
            ..Default::default()
        })),
        ..Default::default()
    }
}

// ── Transform benchmarks ────────────────────────────────────────────────────

fn bench_from_alert(c: &mut Criterion) {
    let msg = alert_msg();
    let alert = match &msg.content {
        Some(Content::Alert(a)) => a,
        _ => unreachable!(),
    };
    let node_id = msg.node_id.as_deref().unwrap();
    c.bench_function("transform::alert::from_alert", |b| {
        b.iter(|| alert::from_alert(black_box(node_id), black_box(alert)))
    });
}

fn bench_to_task(c: &mut Criterion) {
    let cmd = isr_command();
    c.bench_function("transform::task::to_task", |b| {
        b.iter(|| {
            task_transform::to_task(
                black_box("src-uuid"),
                black_box("dst-uuid"),
                black_box(&cmd),
            )
        })
    });
}

// ── route_message benchmarks ────────────────────────────────────────────────

fn bench_route_alert(c: &mut Criterion) {
    let msg = alert_msg();
    c.bench_function("route_message (Alert)", |b| {
        b.iter(|| route_message(black_box(msg.clone()), None, None))
    });
}

fn bench_route_task_ack(c: &mut Criterion) {
    let msg = task_ack_msg("task-bench-001", TaskStatus::Accepted);
    c.bench_function("route_message (TaskAck)", |b| {
        b.iter(|| route_message(black_box(msg.clone()), None, None))
    });
}

fn bench_route_alert_ack(c: &mut Criterion) {
    let msg = SapientMessage {
        timestamp: None,
        node_id: Some("hldmm-bench-001".into()),
        destination_id: None,
        content: Some(Content::AlertAck(AlertAck {
            alert_id: Some("01HZALERTACK00000000000000A".into()),
            alert_ack_status: Some(AlertAckStatus::Accepted as i32),
            ..Default::default()
        })),
        additional_information: None,
    };
    c.bench_function("route_message (AlertAck)", |b| {
        b.iter(|| route_message(black_box(msg.clone()), None, None))
    });
}

// ── TaskQueue benchmarks ────────────────────────────────────────────────────

fn bench_task_queue_enqueue(c: &mut Criterion) {
    c.bench_function("TaskQueue::enqueue", |b| {
        b.iter_custom(|iters| {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_time()
                .build()
                .unwrap();
            rt.block_on(async {
                let mut q = TaskQueue::new(iters as usize + 1, Duration::from_secs(300));
                let start = std::time::Instant::now();
                for i in 0..iters {
                    let id = format!("task-{i}");
                    q.enqueue("node-1", id.clone(), task_sapient_msg(&id));
                }
                start.elapsed()
            })
        });
    });
}

fn bench_task_queue_ack(c: &mut Criterion) {
    c.bench_function("TaskQueue::ack", |b| {
        b.iter_custom(|iters| {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_time()
                .build()
                .unwrap();
            rt.block_on(async {
                let mut q = TaskQueue::new(iters as usize + 1, Duration::from_secs(300));
                let ids: Vec<String> = (0..iters).map(|i| format!("task-{i}")).collect();
                for id in &ids {
                    q.enqueue("node-1", id.clone(), task_sapient_msg(id));
                }
                let start = std::time::Instant::now();
                for id in &ids {
                    q.ack("node-1", id);
                }
                start.elapsed()
            })
        });
    });
}

fn bench_task_queue_pending_for(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    let q = rt.block_on(async {
        let mut q = TaskQueue::new(100, Duration::from_secs(300));
        for i in 0..50 {
            let id = format!("task-{i}");
            q.enqueue("node-1", id.clone(), task_sapient_msg(&id));
        }
        q
    });
    c.bench_function("TaskQueue::pending_for (50 tasks)", |b| {
        b.iter(|| black_box(q.pending_for("node-1")))
    });
}

criterion_group!(transforms, bench_from_alert, bench_to_task,);

criterion_group!(
    routing,
    bench_route_alert,
    bench_route_task_ack,
    bench_route_alert_ack,
);

criterion_group!(
    task_queue,
    bench_task_queue_enqueue,
    bench_task_queue_ack,
    bench_task_queue_pending_for,
);

criterion_main!(transforms, routing, task_queue);
