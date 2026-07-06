# peat-sapient: Implementation & Test Plan

**ADR**: [ADR-070](https://github.com/defenseunicorns/peat/blob/main/docs/adr/070-sapient-protocol-bridge.md)  
**Status**: Active  
**Last updated**: 2026-07-03

---

## Repo structure

Since Phase 6, this repo is a two-crate Cargo workspace:

```
peat-sapient/                (workspace root)
├── peat-sapient/            # the library described below — Layers 1 & 2, plus
│                            # the translator-codec feature (Phase 6)
└── peat-mesh-sapient/       # peat_mesh::transport::Translator/Transport adapter
                             # (ADR-059 Amendment 4 one-way adapter crate — see
                             # Phase 6 below for why it isn't inside peat-sapient)
```

## Crate architecture (`peat-sapient`)

`peat-sapient` has three logical layers, separated by feature flags:

```
peat-sapient
├── Layer 1: General SAPIENT library       (always compiled; no peat-schema dep)
│   ├── proto/          vendored BSI Flex 335 v2.0 proto files
│   ├── src/codec.rs    TCP framing codec (4-byte LE length prefix)
│   ├── src/connection.rs  TCP connection management (HLDMM + DLMM modes)
│   ├── src/proto/      prost-generated types (re-exported)
│   └── src/error.rs    SapientError
│
├── Layer 2: Peat transformer              (feature = "peat"; adds peat-schema dep)
│   ├── src/transform/
│   │   ├── registration.rs   Registration → peat_schema::CapabilityAdvertisement
│   │   ├── status.rs         StatusReport → NodeState + CapabilityAdvertisement delta
│   │   ├── detection.rs      DetectionReport → Track  (+ coord conversion)
│   │   ├── alert.rs          Alert → SapientAlertEvent
│   │   └── task.rs           HierarchicalCommand → Task (outbound)
│   ├── src/bridge.rs     SapientBridge — routes inbound messages, applies rate limiting
│   ├── src/registry.rs   NodeRegistry — per-connection sensor state (Arc<RwLock<_>>)
│   ├── src/rate_limit.rs DetectionLimiter — per-node token-bucket rate limiter
│   └── src/watchdog.rs   run_watchdog — heartbeat-timeout background task
│
└── translator-codec feature (Phase 6)     (feature = "translator-codec"; implies "peat")
    └── src/mesh_fields.rs   peat_schema struct ↔ flat JSON, for peat-mesh-sapient's
                             Translator impl — still zero peat-mesh dependency
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

## Phase 4 — Bridge API & integration tests ✅

**Goal:** `SapientBridge` functional end-to-end against Apex as the SAPIENT counterpart.

### Tasks

- [x] `SapientBridge::new()` — instantiates config + rate limiter; returns `(Self, mpsc::Receiver<SapientUpdate>)`
- [x] `route_message()` — routes inbound content to `SapientUpdate` variants
- [x] Detection rate limiter — token bucket, configurable per-node (`rate_limit.rs`)
- [x] Heartbeat watchdog — emits `NodeDisconnected` after `2 × heartbeat_interval` (`watchdog.rs`)
- [x] Integration test harness — `tests/integration/` with Apex skip guard (feature `integration-tests`)
- [x] `SapientBridge::start()` — HLDMM TCP listener + per-connection routing task loop
- [x] `SapientBridge::send_task()` — enqueue outbound task; send immediately if connected
- [x] DIL outbound task queue — per-node `TaskQueue`; replay on reconnect; TTL expiry with warn (#15)
- [x] `TaskAck` → `SapientUpdate::TaskAcknowledged` — closes command feedback loop; dequeues acked task
- [x] `SapientBridge::registry()` — exposes `NodeRegistry` Arc for watchdog integration

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

## Phase 6 — SAPIENT ↔ CoT via peat-mesh Translator (v1: telemetry only) ✅

**Goal:** SAPIENT `DetectionReport`/`Registration`/`StatusReport` reach CoT/ATAK
consumers (and other mesh nodes) via `peat-mesh`'s `Translator`/`Document`
mechanism (ADR-059), without merging SAPIENT and CoT handling into one crate
(ADR-070 already rejected that) and without a hand-rolled gateway shuttling
between `SapientBridge` and `peat-tak-bridge`.

Per ADR-059 **Amendment 4** (peat repo): SAPIENT is an application-domain-specific
transport (like TAK, unlike BLE), so its `Translator` impl lives in a
**one-way adapter crate**, not behind a `mesh-translator` back-edge feature
inside `peat-sapient` itself (the pattern `CotTranslator` currently uses,
which the amendment deprecates). Kept in-repo as a workspace member rather
than a new repo, since the adapter crate has no existence outside wrapping
`peat-sapient`.

### Repo restructuring

`peat-sapient` became a two-crate Cargo workspace:

```
peat-sapient/                    (workspace root)
├── peat-sapient/                # the library — STILL zero peat-mesh dependency
│   └── src/mesh_fields.rs       # flat-JSON projection (translator-codec feature)
└── peat-mesh-sapient/           # NEW — depends on peat-mesh + peat-sapient, one-way
    └── src/
        ├── translator.rs        # impl Translator for SapientTranslator
        └── transport.rs         # impl MeshTransport/Transport for PeatSapientTransport
```

### Scope (v1 — inbound only)

| SAPIENT message | Mesh collection | Direction |
|---|---|---|
| `DetectionReport` | `tracks` | SAPIENT → mesh |
| `Registration` / `StatusReport` | `platforms` | SAPIENT → mesh |
| `Task` / `TaskAck` | *(none — stays on `SapientBridge`/`TaskQueue`)* | out of scope |

In the initial Phase 6 scope, `SapientTranslator::encode_outbound` always
declined — there is no BSI Flex 335 v2.0 message for "manager pushes a
track to a sensor." Bidirectional `tracks` support was added in Phase 7.

Tasking is ack-correlated and ordered — it stays on `peat-sapient`'s existing
stateful bridge path rather than being flattened into the eventually-consistent
`Document`/CRDT model. See `docs/c2-collaboration.md`'s still-open
`CommandAcknowledgment`/`CommandCoordinator` gap for where real tasking
interop would go.

`platforms` has no CoT-side consumer yet (`CotTranslator` in the `peat` repo
only carries `tracks` today) — its correctness bar here is "lands in the
mesh with the right `Document` shape, proven by tests," not "visible in
ATAK." Extending `CotTranslator` to carry `platforms` is a tracked follow-up
in the `peat` repo.

### Tests

- `peat-sapient/src/mesh_fields.rs` — unit tests for both projection functions.
- `peat-mesh-sapient/src/translator.rs` — unit tests for `decode_inbound`/`encode_outbound`.
- `peat-mesh-sapient/tests/hldmm_integration.rs` — real `peat_mesh::Node` (in-memory backend) + a fake DLMM over loopback TCP; proves the codec is wired correctly end-to-end, not just unit-tested in isolation.

**Gate:** `cargo test --workspace` — 157 tests passing across both crates.

---

## Phase 7 — Bidirectional bridging + bridge binary ✅

**Goal:** CoT/TAK-originated tracks reach a SAPIENT HLDMM as `DetectionReport`s
via the mesh, completing the bidirectional data flow. Shipped as the
`peat-sapient-bridge` binary that composes both `SapientTranslator` and
`CotTranslator` under a single `TransportManager`.

### Scope

| SAPIENT message | Mesh collection | Direction |
|---|---|---|
| `DetectionReport` | `tracks` | **Bidirectional** (encode_outbound in DLMM mode) |
| `Registration` / `StatusReport` | `platforms` | SAPIENT → mesh (no outbound BSI message) |

`SapientTranslator::encode_outbound` now encodes `tracks` documents as
`DetectionReport` protobuf. This makes sense when the peat node acts as a
**virtual DLMM** — forwarding mesh-originated tracks (e.g. from CoT/TAK)
upstream to a SAPIENT HLDMM. Required fields: `doc.id`, `lat`, `lon`.
Optional: `hae`, `sapient_classification`, `sapient_confidence`.

### Components

- **`SapientOutboundSink`** — `OutboundSink` impl; DLMM mode forwards to the
  HLDMM connection via an internal channel, HLDMM mode discards silently.
- **`PeatSapientTransport::outbound_sink()`** — returns the appropriate sink
  for `TransportManager::register_translator` registration.
- **`run_dlmm_peer_loop`** — bidirectional `tokio::select!` loop handling
  both inbound `recv` and outbound channel drain on the same TCP connection.
- **`peat-sapient-bridge` binary** — third workspace member; composes SAPIENT
  + TAK transports, wires both into `TransportManager` fan-out on `tracks`,
  supports CLI + TOML config, TLS for both protocols.
- **`timestamp_ms` normalization** — standardized on `i64` across both
  translators (PR #38).

### Cross-protocol contract tests

`peat-mesh-sapient/tests/cross_protocol_contract.rs` — 5 tests validating
the shared `tracks` schema contract WITHOUT importing `peat-transport`.
Constructs CoT-shaped mesh documents and verifies `SapientTranslator` handles
them correctly, including full round-trip position preservation.

### Tests

- `peat-mesh-sapient/src/translator.rs` — 8 `encode_outbound` unit tests
- `peat-mesh-sapient/tests/dlmm_integration.rs` — outbound sink e2e, bidirectional e2e
- `peat-mesh-sapient/tests/fanout_e2e.rs` — TransportManager fan-out with echo-loop prevention
- `peat-sapient-bridge/tests/bidirectional_fanout.rs` — SAPIENT↔TAK cross-translator round-trip
- `peat-sapient-bridge/tests/network_e2e.rs` — real TAK wire protocol integration

**Gate:** `cargo test --workspace` — 243 tests passing across all three crates.

---

## Phase 8 — DLMM reconnect resilience ✅

**Goal:** When the HLDMM drops the TCP connection, the DLMM transport reconnects
automatically with exponential backoff. The outbound channel survives across
reconnections — messages queued during the outage are flushed on the new
connection.

### Problem

`run_dlmm_connect_loop` established a single connection; when it dropped, the
peer loop consumed `outbound_rx` and exited. The outbound channel was dead
forever — `SapientOutboundSink::send_outbound` returned `Err` and no further
mesh-originated tracks could reach the HLDMM.

### Fix

- `run_dlmm_peer_loop` now returns `mpsc::Receiver<Vec<u8>>` so the outbound
  channel survives reconnections.
- `run_dlmm_connect_loop` (and TLS variant) is now an outer reconnect loop:
  connect → run peer loop → deregister peer → reconnect. Exits only when the
  peer record is deliberately removed (via `disconnect()` or `stop()`).
- New `deregister_peer` helper emits `PeerEvent::Disconnected` between
  connection attempts.
- Messages buffered in the channel during the outage are flushed to the new
  connection on reconnect (bounded at `OUTBOUND_CHANNEL_DEPTH = 64`).

### Tests

- `dlmm_integration.rs::dlmm_reconnects_after_hldmm_drops_connection` —
  drops the HLDMM connection, verifies automatic reconnect, confirms both
  inbound and outbound resume on the new connection.

**Gate:** `cargo test --workspace` — 249 tests passing.

---

## Phase summary

| Phase | Scope | Peat dep? | CI? | Status |
|-------|-------|-----------|-----|--------|
| 1 | Proto bindings | No | Yes | ✅ Done |
| 2 | TCP codec + connection | No | Yes | ✅ Done |
| 3 | Message mapping | Yes (`peat` feature) | Yes | ✅ Done |
| 4 | Bridge API + integration tests | Yes | Yes | ✅ Done |
| 5 | Formal compliance | — | Manual | ⏳ Pending |
| 6 | SAPIENT ↔ CoT via peat-mesh Translator (v1: tracks/platforms inbound) | Yes (`peat-mesh-sapient`) | Yes | ✅ Done |
| 7 | Bidirectional bridging + bridge binary | Yes (3-crate workspace) | Yes | ✅ Done |
| 8 | DLMM reconnect resilience | Yes (`peat-mesh-sapient`) | Yes | ✅ Done |
