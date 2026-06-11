# peat-sapient: Implementation & Test Plan

**ADR**: [ADR-070](https://github.com/defenseunicorns/peat/blob/main/docs/adr/070-sapient-protocol-bridge.md)  
**Status**: Active  
**Last updated**: 2026-06-11

---

## Crate architecture

`peat-sapient` has two logical layers, separated by a feature flag:

```
peat-sapient
├── Layer 1: General SAPIENT library       (always compiled; no peat-schema dep)
│   ├── proto/          vendored BSI Flex 335 v2.0 proto files
│   ├── src/codec.rs    TCP framing codec (4-byte LE length prefix)
│   ├── src/connection.rs  TCP connection management (HLDMM + DLMM modes)
│   ├── src/proto/      prost-generated types (re-exported)
│   └── src/error.rs    SapientError
│
└── Layer 2: Peat transformer              (feature = "peat"; adds peat-schema dep)
    ├── src/transform/
    │   ├── registration.rs   Registration → peat_schema::CapabilityAdvertisement
    │   ├── status.rs         StatusReport → NodeState + CapabilityAdvertisement delta
    │   ├── detection.rs      DetectionReport → Track  (+ coord conversion)
    │   ├── alert.rs          Alert → SapientAlertEvent
    │   └── task.rs           HierarchicalCommand → Task (outbound)
    ├── src/bridge.rs     SapientBridge — routes inbound messages, applies rate limiting
    ├── src/registry.rs   NodeRegistry — per-connection sensor state (Arc<RwLock<_>>)
    ├── src/rate_limit.rs DetectionLimiter — per-node token-bucket rate limiter
    └── src/watchdog.rs   run_watchdog — heartbeat-timeout background task
```

**Layer 1** is a general-purpose Rust SAPIENT library — the first such crate. Useful standalone
for any Rust integrator who speaks SAPIENT but does not use Peat.

**Layer 2** (behind `feature = "peat"`) adds the `peat-schema` dependency and provides the
full bidirectional transformer. The `SapientBridge` lives here and requires `feature = "peat"`.

Default features: `["peat"]` — consumers get the full bridge out of the box;
library-only consumers opt out with `default-features = false`.

---

## Wire protocol

Confirmed from Apex middleware source (`message_io.py`):

```
[ 4 bytes, little-endian uint32: payload length ] [ N bytes: serialized SapientMessage protobuf ]
```

No delimiter or magic bytes. `tokio_util::codec::{Encoder, Decoder}` implementation
wraps this into a `SapientCodec`.

---

## Dstl reference resources

| Resource | URL | Use |
|----------|-----|-----|
| Proto files (BSI Flex 335 v2.0) | https://github.com/dstl/SAPIENT-Proto-Files | Vendored into `proto/bsi_flex_335_v2_0/` |
| Apex middleware (Python ASM broker) | https://github.com/dstl/Apex-SAPIENT-Middleware | Integration test subprocess |
| BSI Flex 335 v2 test harness (C#, Windows) | https://github.com/dstl/BSI-Flex-335-v2-Test-Harness | Manual compliance gate — see `docs/compliance.md` |
| SAPIENT ICD v7 PDF (public) | https://assets.publishing.service.gov.uk/media/6419a2068fa8f547c68029d3/SAPIENT_Interface_Control_Document_v7_FINAL__fixed2_.pdf | Field semantics reference |

**License:** `peat-sapient` is Apache License 2.0. Vendored Dstl proto files are also Apache 2.0 — no compatibility issue.

---

## Phase 1 — Proto bindings & round-trip serialization ✅

**Goal:** `cargo test -p peat-sapient` passes with round-trip tests for all 9 message types.

### Tasks

- [x] Vendor `bsi_flex_335_v2_0/` proto files into `peat-sapient/proto/`
- [x] Write `build.rs` using `prost-build` to generate Rust types
- [x] Re-export generated types from `src/lib.rs` under `peat_sapient::proto`
- [x] Pin proto files to a specific commit (recorded in `proto/VERSION`)

### Tests

`tests/proto_roundtrip.rs` — 10 tests, one per message type. All pass.

---

## Phase 2 — TCP codec & connection layer ✅

**Goal:** Codec round-trips correctly; connection modes start and accept/connect under test.

### Tasks

- [x] `SapientCodec` — encode: serialize + 4-byte LE length; decode: read length, decode bytes
- [x] HLDMM connection: `TcpListener` + `accept`; per-connection framed stream
- [x] DLMM connection: `connect_with_retry` with exponential backoff
- [x] `NodeRegistry`: `Arc<RwLock<HashMap<String, ConnectedNode>>>` with async-safe ops

### Tests

`tests/codec_tests.rs` — 4 tests (encode/decode round-trip, oversized frame rejection, duplex).  
`tests/connection_tests.rs` — 3 tests (backoff arithmetic, single-failure reconnect, multi-failure reconnect).  
`src/registry.rs` — 11 unit tests including concurrent-read deadlock check.

---

## Phase 3 — Message mapping (Layer 2, feature = "peat") ✅

Work through mapping modules in dependency order. Each mapping module has its own
`#[cfg(test)]` block.

### 3a — `registration.rs` ✅

`Registration` → `peat_schema::CapabilityAdvertisement`

### 3b — `status.rs` ✅

`StatusReport` → `(NodeState, Option<CapabilityAdvertisement>)`

### 3c — `detection.rs` ✅

`DetectionReport` → `peat_schema::Track`

Coordinate systems supported: WGS84 LatLng (passthrough), UTM (Snyder series inverse projection).
RangeBearing requires sensor position from `NodeRegistry`; returns `UnsupportedCoordinateSystem`
if registry has no position for the sensor node.

### 3d — `alert.rs` ✅

`Alert` → `SapientAlertEvent` (standalone struct; no peat-schema `Alert` type exists yet —
see issue #8 decision rationale).

### 3e — `task.rs` ✅

`peat_schema::HierarchicalCommand` → `SapientMessage` carrying `Task`.  
Task ID generated as ULID (BSI Flex 335 v2.0 §task_id).

---

## Phase 4 — Bridge API & integration tests 🔄

**Goal:** `SapientBridge` functional end-to-end against Apex as the SAPIENT counterpart.

### Tasks

- [x] `SapientBridge::new()` — instantiates config + rate limiter
- [x] `route_message()` — routes inbound content to `SapientUpdate` variants
- [x] Detection rate limiter — token bucket, configurable per-node (`rate_limit.rs`)
- [x] Heartbeat watchdog — emits `NodeDisconnected` after `2 × heartbeat_interval` (`watchdog.rs`)
- [x] Integration test harness — `tests/integration/` with Apex skip guard (feature `integration-tests`)
- [ ] `SapientBridge::start()` — spawn TCP listener/connector + routing task loop
- [ ] `SapientBridge::send_task()` — enqueue outbound task; observe ack via channel
- [ ] DIL outbound task queue — replay pending tasks on DLMM reconnect (#15)

### Integration tests (`--features integration-tests,peat`)

```
tests/integration/
├── apex_harness.rs     starts/stops Apex subprocess; skip guard if apex.py absent
├── inbound_flow.rs     DLMM → Apex: registration handshake, DetectionReport, drain-route
└── outbound_flow.rs    HLDMM loopback: Task send + TaskAck round-trip (no Apex needed)
```

Apex-dependent tests skip cleanly when `apex.py` is not on PATH.

---

## Phase 5 — Formal compliance (manual gate, not CI) ⏳

Run BSI Flex 335 v2 test harness (C#, Windows) against `peat-sapient` over TCP.

See `docs/compliance.md` for the full procedure.

**Gate:** pass/fail per message type documented in the PR that completes Phase 4.

---

## Phase summary

| Phase | Scope | Peat dep? | CI? | Status |
|-------|-------|-----------|-----|--------|
| 1 | Proto bindings | No | Yes | ✅ Done |
| 2 | TCP codec + connection | No | Yes | ✅ Done |
| 3 | Message mapping | Yes (`peat` feature) | Yes | ✅ Done |
| 4 | Bridge API + integration tests | Yes | Yes | 🔄 In progress |
| 5 | Formal compliance | — | Manual | ⏳ Pending Phase 4 |
