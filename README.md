# peat-sapient

SAPIENT (BSI Flex 335 v2.0) protocol library and Peat mesh bridge.

Provides bidirectional integration between [SAPIENT](https://www.gov.uk/guidance/sapient-autonomous-sensor-system)
sensor/autonomous-platform nodes and the [Peat](https://github.com/defenseunicorns/peat) mesh ecosystem.

## Two layers

**Layer 1 — standalone SAPIENT library** (no Peat dependency):
- prost-generated types for all BSI Flex 335 v2.0 messages
- `tokio_util` codec for the 4-byte LE length-prefix TCP framing
- TCP connection management (HLDMM listener / DLMM client with retry)

**Layer 2 — Peat transformer** (`feature = "peat"`, on by default):
- Bidirectional mappings between SAPIENT messages and `peat-schema` types
- `SapientBridge` with broadcast channel API for mission application integration

### Use as a standalone SAPIENT library

```toml
peat-sapient = { version = "0.1", default-features = false }
```

### Use as a Peat bridge (default)

```toml
peat-sapient = "0.1"
```

## SAPIENT standard

BSI Flex 335 v2.0 — published by the British Standards Institution and Dstl.
Proto definitions vendored from [dstl/SAPIENT-Proto-Files](https://github.com/dstl/SAPIENT-Proto-Files)
(Apache 2.0). See `proto/VERSION` for the upstream commit.

## Architecture decision

See [ADR-070](https://github.com/defenseunicorns/peat/blob/main/docs/adr/070-sapient-protocol-bridge.md).

## License

Apache License 2.0 — see [LICENSE](LICENSE).
