use std::sync::Arc;
use std::time::Duration;

use criterion::{criterion_group, criterion_main, Criterion};
use peat_mesh::sync::{DataSyncBackend, InMemoryBackend, Query};
use peat_mesh::transport::MeshTransport;
use peat_mesh::Node;
use peat_mesh_sapient::{PeatSapientTransport, SapientRole, SapientTranslator};
use peat_sapient::connection;
use peat_sapient::proto::sapient_msg::bsi_flex_335_v2_0::detection_report::LocationOneof;
use peat_sapient::proto::sapient_msg::bsi_flex_335_v2_0::{
    Location, LocationCoordinateSystem, LocationDatum,
};
use peat_sapient::proto::{Content, DetectionReport, SapientMessage};

fn free_local_addr() -> std::net::SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap()
}

fn detection_msg(object_id: &str) -> SapientMessage {
    SapientMessage {
        node_id: Some("sensor-bench".into()),
        content: Some(Content::DetectionReport(DetectionReport {
            object_id: Some(object_id.into()),
            location_oneof: Some(LocationOneof::Location(Location {
                x: Some(-118.25),
                y: Some(34.05),
                z: Some(120.0),
                coordinate_system: Some(LocationCoordinateSystem::LatLngDegM as i32),
                datum: Some(LocationDatum::Wgs84E as i32),
                ..Default::default()
            })),
            ..Default::default()
        })),
        ..Default::default()
    }
}

fn bench_single_detection_latency(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    let (node, mut framed) = rt.block_on(async {
        let backend: Arc<dyn DataSyncBackend> = Arc::new(InMemoryBackend::new_initialized());
        let node = Arc::new(Node::new(backend));
        let translator = Arc::new(SapientTranslator::new());
        let listen_addr = free_local_addr();
        let transport =
            PeatSapientTransport::new(SapientRole::Hldmm { listen_addr }, node.clone(), translator);
        transport.start().await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        let framed = connection::connect(listen_addr).await.unwrap();
        (node, framed)
    });

    let mut counter = 0u64;

    c.bench_function("transport: single DetectionReport TCP → mesh Node", |b| {
        b.iter(|| {
            counter += 1;
            let id = format!("det-{counter}");
            rt.block_on(async {
                connection::send(&mut framed, detection_msg(&id))
                    .await
                    .unwrap();
                loop {
                    let docs = node.query("tracks", &Query::All).await.unwrap();
                    if docs.len() >= counter as usize {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            });
        });
    });
}

fn bench_batch_detection_throughput(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    let (node, mut framed) = rt.block_on(async {
        let backend: Arc<dyn DataSyncBackend> = Arc::new(InMemoryBackend::new_initialized());
        let node = Arc::new(Node::new(backend));
        let translator = Arc::new(SapientTranslator::new());
        let listen_addr = free_local_addr();
        let transport =
            PeatSapientTransport::new(SapientRole::Hldmm { listen_addr }, node.clone(), translator);
        transport.start().await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        let framed = connection::connect(listen_addr).await.unwrap();
        (node, framed)
    });

    let batch_size = 100u64;
    let mut batch_num = 0u64;

    c.bench_function(
        "transport: 100× DetectionReport TCP → mesh Node (batch)",
        |b| {
            b.iter_custom(|iters| {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    batch_num += 1;
                    let base = batch_num * batch_size;
                    let target = (base + batch_size) as usize;

                    let elapsed = rt.block_on(async {
                        let start = std::time::Instant::now();
                        for i in 0..batch_size {
                            let id = format!("det-{}", base + i);
                            connection::send(&mut framed, detection_msg(&id))
                                .await
                                .unwrap();
                        }
                        loop {
                            let docs = node.query("tracks", &Query::All).await.unwrap();
                            if docs.len() >= target {
                                break;
                            }
                            tokio::task::yield_now().await;
                        }
                        start.elapsed()
                    });
                    total += elapsed;
                }
                total
            });
        },
    );
}

criterion_group!(
    transport,
    bench_single_detection_latency,
    bench_batch_detection_throughput
);
criterion_main!(transport);
