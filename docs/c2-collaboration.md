# Peat ↔ SAPIENT C2 Collaboration Model

This document analyses what it means for a Peat-based asset and a Sensor and Platform
Integration Extended from NATO Technology (SAPIENT) asset to collaborate — from both
sides' Command and Control (C2) perspectives. It establishes the conceptual foundation
for Phase 4 implementation decisions, particularly the Disconnected, Intermittent, and
Low-bandwidth (DIL) outbound task queue (issue #15) and the TaskAck →
CommandAcknowledgment feedback path.

---

## Background: The Two Protocols

### Peat

Peat is a full-duplex hierarchical coordination system for distributed autonomous
operations. It is **not** a flat peer mesh. Its hierarchy runs:

```
Node → Cell → Cohort → Federation → Coalition
```

Commands propagate **downward** through the hierarchy; capability advertisements and
detection data aggregate **upward**. The design goal is Joint All-Domain Command and
Control (JADC2) / Multi-Domain Operations (MDO) doctrine applied as software: push
decision authority to the edge so that autonomous operation continues coherently
during network partitions, not merely degrades gracefully.

Key Peat primitives relevant here:

| Primitive | Description |
|-----------|-------------|
| `HierarchicalCommand` | Mission-abstraction command type (MissionOrder, EngagementOrder, FormationChange, ConfigurationUpdate) |
| `CommandAcknowledgment` | Status flow: received → accepted → executing → completed / failed / rejected |
| `CapabilityAdvertisement` | What a Node can do; flows up the hierarchy for aggregation |
| `Track` | A detected object with position, classification, confidence, velocity |
| `NodeState` / `NodeHealth` | Operational status and health of a mesh node |
| Conflict-free Replicated Data Type (CRDT) sync | Automerge / Iroh-backed distributed state that resolves concurrent writes |

### SAPIENT

SAPIENT (BSI Flex 335 v2.0) is a UK Ministry of Defence (MoD) open standard,
developed and maintained by the Defence Science and Technology Laboratory (Dstl), for
integrating heterogeneous sensors and autonomous platforms into C2 systems. It is
progressing through North Atlantic Treaty Organization (NATO) standardisation.

SAPIENT defines two logical roles:

| Role | Term | Description |
|------|------|-------------|
| Manager | High-Level Decision Making Module (HLDMM) | Issues tasks; receives detections, status, and alerts |
| Sensor / autonomous platform | Detection-Level Multi-sensor Management Module (DLMM) | Registers, reports status, emits detections, accepts tasks |

An optional Autonomous System Manager (ASM) broker sits between HLDMMs and DLMMs in
hub-spoke deployments. The wire protocol is length-prefixed Protocol Buffers over
Transmission Control Protocol (TCP).

SAPIENT message vocabulary:

| Message | Direction | Purpose |
|---------|-----------|---------|
| `Registration` | DLMM → HLDMM | Announces identity, node type, initial capabilities |
| `StatusReport` | DLMM → HLDMM | Periodic heartbeat with operational status and health |
| `DetectionReport` | DLMM → HLDMM | Sensor detection event: location, classification, confidence |
| `Alert` | DLMM → HLDMM | Out-of-band alert (intrusion, fault, boundary crossing) |
| `Task` | HLDMM → DLMM | Tasking command: scan, track, follow, alert-on, idle |
| `TaskAck` | DLMM → HLDMM | Task acceptance or rejection with reason |
| `RegistrationAck` | HLDMM → DLMM | Confirms registration accepted |
| `Error` | either | Error notification |

---

## The Bridge: `peat-sapient`

`peat-sapient` is the protocol bridge between these two models. Its role is **semantic
translation** — not tunnelling Peat sync bytes through SAPIENT, but mapping SAPIENT's
sensor/platform vocabulary into Peat document types and vice versa. It follows the same
external-crate composition pattern as `peat-mavlink`.

The bridge node (the Peat node running `peat-sapient`) occupies the **Cell** level of
the Peat hierarchy for the sensors it manages. It presents as a single HLDMM to all
connected DLMMs; the Peat mesh behind it is invisible to those sensors.

---

## C2 Perspectives

### Peat's View of SAPIENT Sensors

A SAPIENT DLMM appears to the Peat mesh as a **leaf Node** with a specialised
capability profile. The integration proceeds in two directions simultaneously.

#### Upward data flow (sensor → mesh)

When a DLMM connects and sends `Registration`, `peat-sapient` produces a
`CapabilityAdvertisement` that flows up the Cell → Cohort → Federation chain. Higher
levels of the hierarchy gain awareness of which sensors exist at the edge, what they
can detect, and what tasks they accept — without those higher levels needing any
knowledge of SAPIENT.

Subsequent messages continue the upward flow:

| SAPIENT message | Peat result | Flows up as |
|----------------|-------------|-------------|
| `Registration` | `CapabilityAdvertisement` | Sensor capability, node type, Field of View (FOV) |
| `StatusReport` | `NodeState` + optional `CapabilityAdvertisement` delta | Health, operational mode |
| `DetectionReport` | `Track` | Position (World Geodetic System 84 / WGS84), classification, confidence, velocity |
| `Alert` | `SapientAlertEvent` | Alert type, severity, position |

Detection data is especially significant: SAPIENT's native hub-spoke topology traps
sensor detections inside a single ASM. Peat propagates them across the full mesh — any
node at any hierarchy level can subscribe to `Track` updates from any SAPIENT source.
This is a material capability advantage over operating the same sensors in a
SAPIENT-only deployment.

#### Downward command flow (mesh → sensor)

A `HierarchicalCommand` issued at any authority level above the bridge node — Cohort,
Federation, or Coalition — routes down through the Peat hierarchy to the bridge node.
The bridge translates it into a SAPIENT `Task` and delivers it to the targeted DLMM.

The translation is necessarily lossy: Peat's command vocabulary is mission-abstraction
level (MissionOrder, EngagementOrder, FormationChange, ConfigurationUpdate); SAPIENT's
`Task` is capability-specific (scan this region, track this object, follow this
bearing, alert-on this condition). A rich translation therefore keys off the DLMM's
registered `CapabilityAdvertisement` — which task parameters are appropriate depends on
what the sensor can do.

#### Command feedback (TaskAck → CommandAcknowledgment)

The feedback loop is currently incomplete. When a DLMM sends `TaskAck`, the bridge
routes it to `SapientUpdate::Ignored`. The correct path is:

```
TaskAck (Accepted)   →  CommandAcknowledgment::Accepted  →  up the Peat hierarchy
TaskAck (Rejected)   →  CommandAcknowledgment::Rejected  →  up the Peat hierarchy
```

Closing this loop allows the issuing authority (whatever Cohort or Federation node
originated the command) to track command lifecycle via Peat's standard acknowledgment
state machine, without knowing that the underlying asset is a SAPIENT sensor.

#### Multi-issuer conflict resolution

Peat distributes authority: multiple nodes at different hierarchy levels can
legitimately issue a `HierarchicalCommand` targeting the same sensor. The Peat CRDT
conflict resolver applies the configured policy (highest-priority-wins,
highest-authority-wins, etc.) **before** the winning command reaches the bridge. The
bridge therefore always sends one resolved task to the DLMM — which is correct SAPIENT
behaviour. The HLDMM role is singular on the wire; the distribution happens inside the
mesh.

### SAPIENT's View of Peat

A SAPIENT DLMM sees a single HLDMM at the bridge's TCP endpoint. The mesh behind it is
invisible. From the DLMM's perspective, nothing changes from a native SAPIENT
deployment: it registers, reports status, emits detections, receives tasks, and sends
`TaskAck`. The bridge is the boundary.

What is different — invisibly to the DLMM — is that its manager is distributed. The
HLDMM can receive commands from an operator at Federation level, resolve conflicts
between two simultaneously-issued tasks, and propagate the DLMM's detections to peers
across the mesh that the DLMM has never connected to. The sensor gains mesh-scale
situational awareness propagation without any firmware changes.

---

## Key Design Tensions

### 1. DIL Semantics: Where the Models Diverge Most

SAPIENT has no concept of a disconnected HLDMM. If the manager disappears, the DLMM is
unmanaged: it continues executing its last-acknowledged task (implementation-defined
behaviour) until it reconnects or is power-cycled.

Peat **explicitly** designs for partition-tolerant autonomy. ADR-009 §4 describes
pre-positioning decision logic at the edge so that autonomous operation continues as
commanded intent, not undefined behaviour, during network partitions. The DIL outbound
task queue (issue #15) is the concrete expression of this principle for SAPIENT sensors:

- Tasks are queued with a Time-to-Live (TTL) before being sent.
- A task is dequeued only when `TaskAck::Accepted` is received, confirming the DLMM
  has acknowledged it.
- If the Peat link is partitioned after the DLMM has acknowledged the task, the
  sensor continues operating on that intent.
- If the Peat link is partitioned before acknowledgment (e.g., the bridge crashes
  between sending the task and receiving `TaskAck`), the queue replays the task on
  reconnect.
- TTL expiry emits a warning rather than replaying a stale command — a task whose
  operational context has expired is worse than no task.

This makes the bridge a pre-positioning point in the Peat hierarchy: the last
authorised task state for each DLMM is maintained at the bridge so the sensor's
autonomous behaviour remains mission-aligned through intermittent connectivity.

### 2. Command Semantics: Lossy in One Direction

Translating **from** Peat's mission-abstraction commands **to** SAPIENT's
capability-specific tasks is lossy. A `MissionOrder` does not specify scan parameters,
detection thresholds, or task regions; a SAPIENT `Task` must. The bridge must supply
those parameters from:

- The DLMM's registered capabilities (`CapabilityAdvertisement`) — what the sensor can do.
- A configurable command profile — what defaults to apply per `CommandType`.
- Optional fields carried in `HierarchicalCommand.mission_order` or equivalent.

Translating **from** SAPIENT `TaskAck` / `StatusReport` **to** Peat's
`CommandAcknowledgment` / `NodeState` is straightforward and non-lossy.

This asymmetry means the bridge is most naturally exercised in the **Peat as
HLDMM** direction — Peat issues high-level intent; SAPIENT sensors execute it as
capability-specific actions. The reverse (SAPIENT HLDMM tasking a Peat-presented DLMM)
is a distinct, not-yet-implemented scenario that would require the bridge to:

1. Register the Peat asset as a DLMM to an external HLDMM.
2. Translate incoming SAPIENT `Task` messages into `HierarchicalCommand` events for
   the Peat mesh.
3. Generate `TaskAck` in response to those commands.

That direction requires an explicit design decision on authority: who has precedence
when both the Peat mesh hierarchy and an external SAPIENT HLDMM can task the same
asset simultaneously? Until that decision is recorded, Direction B (Peat as DLMM)
should not be implemented.

### 3. Heartbeat Timeout and Node Liveness

Both protocols have heartbeat mechanisms:

- SAPIENT: DLMMs send `StatusReport` on a cadence advertised in `Registration`. The
  bridge marks a node `NodeDisconnected` after `2 × heartbeat_interval` without a
  status update (implemented in `watchdog.rs`).
- Peat: `NodeState` carries a health status; the hierarchy tracks node liveness at each
  aggregation level.

The bridge is responsible for both: it maintains the SAPIENT-side heartbeat watch and
propagates liveness events into the Peat hierarchy as `NodeState` updates. A SAPIENT
node going silent is not just a local event — it changes the capability picture at
every hierarchy level above the bridge.

### 4. Detection Rate and Mesh Bandwidth

Dense surveillance sensors — wide-area cameras, radar arrays — can produce hundreds of
`DetectionReport` messages per second. The Peat mesh links connecting the bridge node
to the wider hierarchy may be bandwidth-constrained (DIL environments, satellite
uplinks, Long Range Radio (LoRa) hops). The per-node token-bucket rate limiter
(implemented in `rate_limit.rs`) addresses this: excess detections become
`SapientUpdate::Ignored` at the bridge rather than flooding the mesh.

The rate limit is a policy decision, not a technical limitation. Setting it correctly
requires knowing:

- The mesh link's sustainable throughput.
- The operational importance of high-frequency detection data vs. aggregated track
  updates.
- Whether fused track state (one `Track` per object, continuously updated) is
  preferable to raw detection streams (one `Track` per detection event).

---

## What Currently Works vs. What Is Missing

### Implemented (Phases 1–4 partial)

| Capability | Location |
|-----------|----------|
| SAPIENT `Registration` → `CapabilityAdvertisement` | `transform/registration.rs` |
| SAPIENT `StatusReport` → `NodeState` + capability delta | `transform/status.rs` |
| SAPIENT `DetectionReport` → `Track` (WGS84, UTM, range/bearing) | `transform/detection.rs` |
| SAPIENT `Alert` → `SapientAlertEvent` | `transform/alert.rs` |
| Peat `HierarchicalCommand` → SAPIENT `Task` | `transform/task.rs` |
| Per-node detection rate limiter | `rate_limit.rs` |
| Heartbeat watchdog → `NodeDisconnected` | `watchdog.rs` |
| Integration test harness (Apex SAPIENT Middleware skip guard) | `tests/integration/` |

### Not Yet Implemented

| Capability | Blocking on | Issue |
|-----------|------------|-------|
| `SapientBridge::start()` — TCP lifecycle loop | Phase 4 | #15 (prerequisite) |
| `SapientBridge::send_task()` — enqueue + await `TaskAck` | `start()` | #15 |
| DIL outbound task queue — replay on reconnect, TTL expiry | `send_task()` | #15 |
| `TaskAck` → `CommandAcknowledgment` feedback path | design decision | — |
| Direction B: Peat as DLMM (register with external HLDMM, receive tasks) | authority ADR | — |

---

## Summary

A Peat-based asset and a SAPIENT-based asset collaborate through `peat-sapient`, which
acts as the bridge between SAPIENT's hub-spoke sensor management topology and Peat's
hierarchical distributed C2 mesh. The primary operational direction is **Peat as the
distributed HLDMM**: SAPIENT DLMMs feed detection data up the Peat hierarchy as
`Track` objects and `CapabilityAdvertisement` records; `HierarchicalCommand` flows
down from any authority level in the hierarchy and is translated into SAPIENT `Task`
messages at the bridge.

The most important design gap relative to Peat's intended model is the DIL task queue:
without it, the bridge's SAPIENT sensors are unmanaged during Peat network partitions,
which violates the pre-positioning / edge-autonomy principle that Peat was built for.
Issue #15 closes that gap.

The second gap is the `TaskAck` → `CommandAcknowledgment` feedback path. Without it,
the Peat hierarchy cannot track whether a command was accepted or rejected by the
underlying sensor, breaking the audit and retry logic that `CommandCoordinator` depends
on.

Both gaps are Phase 4 work. Neither requires an architectural change — only
implementation of the planned `start()` / `send_task()` methods.
