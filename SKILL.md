# SKILL.md — peat-sapient

## Repo role

`peat-sapient` is the SAPIENT (BSI Flex 335 v2.0) protocol library for the Peat ecosystem.
It has two independent layers:

**Layer 1** — standalone SAPIENT library (always compiled).  
Prost-generated proto types, a 4-byte LE length-prefix codec, and TCP connection helpers.
No peat-schema dependency.

**Layer 2** — Peat transformer (feature `peat`, on by default).  
Bidirectional SAPIENT ↔ peat-schema type mappings in `src/transform/`, plus the
`SapientBridge` router in `src/bridge.rs`.

## Language and toolchain

- **Rust** (edition 2021, resolver 2)
- `peat-schema` optional dep via feature `peat`
- `protoc-bin-vendored` for Layer 1 build; system `protoc` also required when building with `--features peat` because `peat-schema`'s own `build.rs` calls it

## Sanity check

```
cargo check -p peat-sapient                          # Layer 1
cargo check -p peat-sapient --features peat          # Layer 2
```

## Verification checklist

Every task in this repo must produce evidence from the following commands before it is done:

```
cargo test --features peat                           # 75 tests must pass
cargo clippy --all-targets --features peat -- -D warnings   # zero warnings
cargo fmt --check                                    # no diffs
```

All three must exit 0.

## Hard rules (inherited from peat ecosystem)

- **FIPS-approved cryptographic primitives only.** See `peat/CLAUDE.md` for the full list.
  Do not introduce ChaCha20-Poly1305, BLAKE2/3, or any non-NIST primitive.
- **No consumer-specific references in code, comments, or tests.**
  Use "consumer", "sensor", "DLMM", "HLDMM" — not vendor/app names.

## Architecture notes

### Layer 1 proto path

All generated types live under:
```
crate::proto::sapient_msg::bsi_flex_335_v2_0::{...}
```
Key types: `SapientMessage`, `Registration`, `StatusReport`, `DetectionReport`, `Task`, `TaskAck`, `Alert`, `AlertAck`.
Content oneof: `sapient_message::Content`.

### Layer 2 transform modules

| Module | Direction | peat-schema output |
|---|---|---|
| `transform::registration` | Registration → | `CapabilityAdvertisement` |
| `transform::status` | StatusReport → | `(NodeState, Option<CapabilityAdvertisement>)` |
| `transform::detection` | DetectionReport → | `Track` |
| `transform::task` | HierarchicalCommand → | `SapientMessage(Task)` |
| `transform::alert` | Alert → | stub (P3d) |

`bridge::route_message` dispatches inbound `SapientMessage` → `SapientUpdate`.

### Coordinate systems (`transform::detection`)

- `LatLngDegM` — passthrough (x=lon, y=lat)
- `LatLngRadM` — radians → degrees
- `UtmM` — inline WGS84 Transverse Mercator inverse (Snyder series, accurate <1m for E<3°)
- `RangeBearing` — flat-earth ENU offset from sensor position; caller must supply `Option<&TrackPosition>`.
  Returns `Err(UnsupportedCoordinateSystem)` when sensor position is absent.

### ULID generation

`task::to_task` generates BSI-mandatory ULID task_ids via the `ulid` crate (optional dep under `peat` feature).

## Skill router

For other repos in the ecosystem see their own `SKILL.md`:

| Repo | Role |
|---|---|
| `peat` | Top-level crate; shared types, traits, errors |
| `peat-schema` | Protobuf schema; `peat-schema/SKILL.md` |
| `peat-node` | Node daemon; `peat-node/SKILL.md` |
| `peat-sapient` | This repo |

## ADR references

- **ADR-070** — SAPIENT integration design, two-layer architecture, P0–P4 phases.
