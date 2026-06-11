# peat-sapient

SAPIENT (BSI Flex 335 v2.0) protocol library and Peat mesh bridge.

Provides bidirectional integration between [SAPIENT](https://www.gov.uk/guidance/sapient-autonomous-sensor-system)
sensor/autonomous-platform nodes and the [Peat](https://github.com/defenseunicorns/peat) mesh ecosystem.

- [Quick start](#quick-start)
- [Two layers](#two-layers)
- [Event model — SapientUpdate](#event-model--sapientupdate)
- [Configuration](#configuration)
- [Coordinate systems](#coordinate-systems)
- [Sending tasks](#sending-tasks)
- [Running tests](#running-tests)
- [Documentation](#documentation)

---

## Quick start

### DLMM mode — connect to an HLDMM and receive events

```rust
use std::net::SocketAddr;
use peat_sapient::{
    bridge::{route_message, SapientUpdate},
    connection::{self, ReconnectConfig},
    registry::{new_registry, upsert, get_position},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr: SocketAddr = "127.0.0.1:5066".parse()?;
    let registry = new_registry();

    let config = ReconnectConfig::default(); // 1 s initial backoff, 30 s max
    let mut framed = connection::connect_with_retry(addr, &config).await?;

    while let Some(msg) = connection::recv(&mut framed).await? {
        let node_id = msg.node_id.clone().unwrap_or_default();

        // Look up last-known sensor position for range-bearing resolution
        let sensor_pos = get_position(&registry, &node_id).await;

        match route_message(msg, sensor_pos.as_ref(), None)? {
            SapientUpdate::Registered { node_id, advertisement } => {
                upsert(&registry, &node_id, Some(advertisement), None).await;
                println!("sensor registered: {node_id}");
            }
            SapientUpdate::StatusUpdated { node_id, state, .. } => {
                println!("status from {node_id}: {:?}", state.health_status);
            }
            SapientUpdate::Detected { node_id, track } => {
                if let Some(pos) = track.position {
                    println!("detection from {node_id}: lat={} lon={}", pos.latitude, pos.longitude);
                }
            }
            SapientUpdate::Alerted { node_id, event } => {
                println!("alert from {node_id}: {} — {}", event.alert_type, event.status);
            }
            SapientUpdate::NodeDisconnected { node_id } => {
                println!("sensor timed out: {node_id}");
            }
            SapientUpdate::Ignored { .. } => {}
        }
    }
    Ok(())
}
```

### HLDMM mode — accept incoming sensor connections

```rust
use peat_sapient::connection;
use tokio::net::TcpListener;

let listener = TcpListener::bind("0.0.0.0:5066").await?;
loop {
    let (mut framed, _peer) = connection::accept(&listener).await?;
    tokio::spawn(async move {
        while let Ok(Some(msg)) = connection::recv(&mut framed).await {
            // handle msg …
        }
    });
}
```

### Layer 1 only — raw SAPIENT without Peat

```toml
peat-sapient = { version = "0.1", default-features = false }
```

```rust
use peat_sapient::connection::{self, ReconnectConfig};

let mut framed = connection::connect_with_retry(addr, &ReconnectConfig::default()).await?;
while let Some(msg) = connection::recv(&mut framed).await? {
    println!("received node_id={:?}", msg.node_id);
}
```

---

## Two layers

| Layer | Feature flag | Contents | Peat dependency |
|-------|-------------|----------|-----------------|
| **1 — SAPIENT library** | always compiled | Proto types, TCP codec, connection management | None |
| **2 — Peat bridge** | `peat` (default) | Message transforms, `SapientBridge`, `NodeRegistry`, rate limiter, watchdog | `peat-schema` |

The two-layer design lets teams use `peat-sapient` as a general Rust SAPIENT library
independent of the broader Peat ecosystem.

---

## Event model — `SapientUpdate`

`route_message(msg, sensor_position, detection_limiter)` maps one inbound `SapientMessage`
to a `SapientUpdate`. All unhandled message types (e.g. `TaskAck`, `RegistrationAck`)
produce `Ignored` rather than an error, so an unexpected message never panics the receive loop.

| Variant | Triggered by | Key fields |
|---------|-------------|-----------|
| `Registered` | `Registration` | `node_id`, `advertisement: CapabilityAdvertisement` |
| `StatusUpdated` | `StatusReport` | `node_id`, `state: NodeState`, optional `capability_delta` |
| `Detected` | `DetectionReport` | `node_id`, `track: Track` (WGS84 position, velocity, classification) |
| `Alerted` | `Alert` | `node_id`, `event: SapientAlertEvent` (type, status, priority, position, attributes JSON) |
| `NodeDisconnected` | Watchdog timeout | `node_id` — node already removed from `NodeRegistry` |
| `Ignored` | Everything else | `reason: String` |

---

## Configuration

### `BridgeConfig`

```rust
use std::time::Duration;
use peat_sapient::bridge::BridgeConfig;
use peat_sapient::rate_limit::RateLimitConfig;

let config = BridgeConfig {
    node_id: "your-hldmm-uuid".into(),
    addr: "0.0.0.0:5066".parse().unwrap(),
    // Per-node DetectionReport rate limit. None = unlimited.
    detection_rate_limit: Some(RateLimitConfig {
        max_per_second: 10.0,
        burst_size: 20,
    }),
    // Nodes silent for 2 × this duration emit NodeDisconnected.
    heartbeat_interval: Duration::from_secs(30), // SAPIENT ICD default
};
```

### Heartbeat watchdog

The watchdog runs as an independent task and emits `NodeDisconnected` events over a channel:

```rust
use peat_sapient::{bridge::SapientUpdate, registry::new_registry, watchdog::run_watchdog};
use tokio::sync::mpsc;
use std::time::Duration;

let registry = new_registry();
let (tx, mut rx) = mpsc::channel(64);

tokio::spawn(run_watchdog(registry.clone(), Duration::from_secs(30), tx));

while let Some(event) = rx.recv().await {
    if let SapientUpdate::NodeDisconnected { node_id } = event {
        println!("sensor {node_id} timed out — removing from downstream state");
    }
}
```

### Detection rate limiter

```rust
use peat_sapient::rate_limit::{DetectionLimiter, RateLimitConfig};

let limiter = DetectionLimiter::new(RateLimitConfig {
    max_per_second: 5.0,
    burst_size: 10,
});

// Pass to route_message — excess detections become SapientUpdate::Ignored
route_message(msg, sensor_pos, Some(&limiter))?;
```

Set `max_per_second: 0.0` or `burst_size: 0` to disable limiting without removing the
`DetectionLimiter` from the call site.

### DLMM reconnect policy

```rust
use peat_sapient::connection::ReconnectConfig;
use std::time::Duration;

let policy = ReconnectConfig {
    initial_delay: Duration::from_millis(500),
    max_delay: Duration::from_secs(60),
};
```

---

## Coordinate systems

| System | Support | Notes |
|--------|---------|-------|
| WGS84 LatLng (`LatLngDegM`) | Full | `x` = longitude °, `y` = latitude °, `z` = altitude m |
| UTM | Full | Snyder series inverse projection → WGS84. Grid zone parsed from `coordinate_system`. |
| Range/bearing (`RangeBearing`) | Requires sensor position | Pass the sensor's `last_position` from `NodeRegistry`; returns `UnsupportedCoordinateSystem` if absent. |
| MGRS | Not supported | Planned — tracked in `detection.rs`. |

---

## Sending tasks

Build a `SapientMessage` carrying a `Task` from a `peat_schema::HierarchicalCommand`:

```rust
use peat_sapient::transform::task::to_task;
use peat_sapient::connection;

let task_msg = to_task(&hldmm_node_id, &sensor_node_id, &command)?;
connection::send(&mut framed, task_msg).await?;
```

`to_task` generates a ULID task ID (BSI Flex 335 v2.0 §task_id) and sets `Control::Start`.
A higher-level `send_task()` method with DIL queuing and acknowledgement tracking is
planned in Phase 4 (see [docs/PLAN.md](docs/PLAN.md)).

---

## Running tests

```sh
# Unit + codec + connection tests — no external tools required
cargo test --features peat

# Integration tests against the Dstl Apex SAPIENT middleware
# Two loopback tests always run; Apex-dependent tests skip if apex.py is absent.
cargo test --features integration-tests,peat --test integration
```

See [docs/developer-guide.md](docs/developer-guide.md) for how to install Apex and run
the full integration suite.

---

## Documentation

| Document | Contents |
|----------|---------|
| [docs/PLAN.md](docs/PLAN.md) | Phase-by-phase implementation plan and current status |
| [docs/compliance.md](docs/compliance.md) | BSI Flex 335 v2 test harness (manual compliance gate) |
| [docs/developer-guide.md](docs/developer-guide.md) | Architecture, transforms, contributing |
| [ADR-070](https://github.com/defenseunicorns/peat/blob/main/docs/adr/070-sapient-protocol-bridge.md) | Architecture decision record |

---

## SAPIENT standard

BSI Flex 335 v2.0 — published by the British Standards Institution and Dstl.
Proto definitions vendored from [dstl/SAPIENT-Proto-Files](https://github.com/dstl/SAPIENT-Proto-Files)
(Apache 2.0). See `proto/VERSION` for the upstream commit.

## License

Apache License 2.0 — see [LICENSE](LICENSE).
