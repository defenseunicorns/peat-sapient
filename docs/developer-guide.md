# peat-sapient Developer Guide

This guide covers the internal architecture, how message transforms work, how to add new
mappings, and how to run the full test suite including integration tests against Apex.

---

## Architecture overview

```
src/
├── lib.rs            Public re-exports; feature gates for Layer 2 modules
├── error.rs          SapientError — all error variants
├── codec.rs          SapientCodec: tokio_util Encoder/Decoder for 4-byte LE framing
├── connection.rs     connect / accept / send / recv; ReconnectConfig; BridgeRole
├── proto/            prost-generated types (re-exported from peat_sapient::proto)
│
│   ── Layer 2 (feature = "peat") ──────────────────────────────────────────────
├── bridge.rs         route_message(); SapientBridge; SapientUpdate enum; BridgeConfig
├── registry.rs       NodeRegistry (Arc<RwLock<HashMap>>); ConnectedNode; CRUD helpers
├── rate_limit.rs     DetectionLimiter; TokenBucket; RateLimitConfig
├── watchdog.rs       run_watchdog — heartbeat timeout → NodeDisconnected
└── transform/
    ├── mod.rs
    ├── registration.rs   Registration → CapabilityAdvertisement
    ├── status.rs         StatusReport → NodeState + optional CapabilityAdvertisement
    ├── detection.rs      DetectionReport → Track (+ coordinate conversion)
    ├── alert.rs          Alert → SapientAlertEvent
    └── task.rs           HierarchicalCommand → SapientMessage(Task)
```

### Data flow — inbound

```
TCP bytes
  → SapientCodec (decode)
  → SapientMessage
  → route_message(msg, sensor_pos, rate_limiter)
  → SapientUpdate variant
  → application
```

The `route_message` free function is the single dispatch point. It is synchronous and
pure (no I/O, no shared state except the rate limiter's interior mutex), which makes it
straightforward to unit-test.

### Data flow — outbound

```
HierarchicalCommand (peat-schema)
  → transform::task::to_task()
  → SapientMessage(Task)
  → SapientCodec (encode)
  → TCP bytes
```

---

## Transform modules

Each module in `src/transform/` handles one SAPIENT message type. They are stateless
free functions that take proto-generated types and return peat-schema types (or custom
event structs where no peat-schema equivalent exists).

### `registration.rs`

```
Registration → CapabilityAdvertisement
```

Maps `NodeDefinition.node_type` to `capability.node_class`, preserves FOV and
detection capabilities in `capabilities: Vec<Capability>`.

Key function: `from_registration(node_id: &str, reg: &Registration) -> CapabilityAdvertisement`

### `status.rs`

```
StatusReport → (NodeState, Option<CapabilityAdvertisement>)
```

The `NodeState` is always produced. `CapabilityAdvertisement` is `Some` only when the
status report carries mode or FOV data that represents a delta from the registration.

Key function: `from_status_report(node_id: &str, sr: &StatusReport) -> (NodeState, Option<CapabilityAdvertisement>)`

### `detection.rs`

```
DetectionReport → Track
```

This is the most complex transform. Coordinate conversion:

- `LatLngDegM` — passthrough (no conversion needed)
- `UTM` — Snyder series Transverse Mercator inverse projection to WGS84
- `RangeBearing` — requires sensor position from the caller (`sensor_position` param);
  returns `SapientError::UnsupportedCoordinateSystem` if absent

Velocity is mapped from `EnuVelocity` (East-North-Up, m/s) to `TrackVelocity`.

Key function: `from_detection_report(node_id: &str, sensor_pos: Option<&TrackPosition>, dr: &DetectionReport) -> Result<Track, SapientError>`

### `alert.rs`

```
Alert → SapientAlertEvent
```

`SapientAlertEvent` is a crate-local struct (not a peat-schema type) because
`peat_schema::AlertProduct` models ML-output triggers, which is semantically different
from SAPIENT's severity-based alert. This was a deliberate design decision in issue #8.

Position conversion uses the same `location_to_track_position` helper from `detection.rs`.
`RangeBearing` without sensor position silently yields `position: None` (infallible).

Key function: `from_alert(node_id: &str, msg: &Alert) -> SapientAlertEvent`

### `task.rs`

```
HierarchicalCommand → SapientMessage(Task)
```

Generates a ULID task ID (`ulid::Ulid::new().to_string()`), sets `Control::Start`,
and maps `CommandType` variants to `task_name`.

Key function: `to_task(source_node_id: &str, destination_node_id: &str, cmd: &HierarchicalCommand) -> Result<SapientMessage, SapientError>`

---

## Adding a new message mapping

1. **Check the proto types.** The generated Rust types live in
   `target/debug/build/peat-sapient-*/out/sapient_msg.bsi_flex_335_v2_0.rs`.
   Verify exact field names before writing the transform — proto field names sometimes
   differ from what you'd expect (e.g. `object_id` not `detection_id`).

2. **Create `src/transform/<type>.rs`.**
   Follow the existing module pattern: one public function, inline `#[cfg(test)]` module
   with tests for every documented field mapping.

3. **Add the route in `bridge.rs`.**
   Match the new `Content::Foo(msg)` arm, call your transform, and return the appropriate
   `SapientUpdate` variant (adding the variant if needed).

4. **Wire the module** in `src/transform/mod.rs` and `src/lib.rs` (behind `#[cfg(feature = "peat")]`).

5. **Run the full suite:**
   ```sh
   cargo test --features peat
   cargo clippy --all-targets --features peat -- -D warnings
   cargo fmt --check
   ```

---

## NodeRegistry

`NodeRegistry = Arc<RwLock<HashMap<String, ConnectedNode>>>` stores per-connection sensor
state. The key is the SAPIENT `node_id` (UUID string from `SapientMessage.node_id`).

```rust
pub struct ConnectedNode {
    pub node_id: String,
    pub capability: Option<CapabilityAdvertisement>,
    pub last_position: Option<TrackPosition>,  // used for range-bearing resolution
    pub last_seen: tokio::time::Instant,        // used by the heartbeat watchdog
}
```

`last_seen` uses `tokio::time::Instant` (not `std::time::Instant`) so that watchdog
tests can use `tokio::time::pause()` and `advance()` without real sleep.

Helper functions (`upsert`, `get_position`, `get_node`, `remove`) take `&NodeRegistry`
and acquire the appropriate read/write lock internally. The write lock is held only for
the duration of the map mutation — no async work is done while holding it.

---

## Rate limiter

`DetectionLimiter` is a per-node token bucket. Each sensor node gets its own `TokenBucket`
entry in an internal `Mutex<HashMap>`. The mutex is a `std::sync::Mutex` (not `tokio`) because
`try_consume` is a pure arithmetic operation with no async work.

Setting `max_per_second = 0.0` or `burst_size = 0` disables the limiter globally for that
`DetectionLimiter` instance at construction time (`enabled: bool` field).

---

## Watchdog

`run_watchdog(registry, interval, tx)` is a `tokio::time::interval`-driven loop. It fires
every `interval`, reads the registry under a shared read lock, collects expired node IDs,
then removes each one and sends `SapientUpdate::NodeDisconnected` over the channel.

The watchdog stops when `tx` is dropped (channel closed).

Tests use `#[tokio::test(start_paused = true)]` + `tokio::time::advance()` — no real sleep.

---

## Running integration tests

Integration tests live in `tests/integration/` and require `--features integration-tests,peat`.

```sh
cargo test --features integration-tests,peat --test integration
```

### Apex-dependent tests

Three inbound tests and one outbound test connect to a live Apex subprocess. They
automatically skip when `apex.py` is not on PATH.

To run them:

1. Clone and install [Apex SAPIENT Middleware](https://github.com/dstl/Apex-SAPIENT-Middleware):
   ```sh
   pip install -e .
   ```
2. Verify `apex.py --version` succeeds.
3. Re-run the integration suite — the Apex tests will execute instead of skipping.

### Loopback tests (no Apex needed)

`task_sent_and_task_ack_received_loopback` and `task_fields_survive_codec_round_trip`
spin up two in-process TCP endpoints. They always run and verify the full
`to_task → codec → route_message` path.

---

## CI gates

Every PR must pass:

```sh
cargo test --features peat
cargo clippy --all-targets --features peat -- -D warnings
cargo fmt --check
```

The integration test binary is not built in standard CI (no `apex.py` available).
It is built and run manually or in a dedicated CI environment with Apex installed.

Formal BSI Flex 335 v2 compliance (C# test harness on Windows) is a manual gate — see
[docs/compliance.md](compliance.md).
