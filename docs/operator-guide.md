# peat-sapient-bridge Operator Guide

peat-sapient-bridge is a standalone service that connects BSI Flex 335 v2.0
(SAPIENT) sensor networks to the Peat mesh. It optionally bridges TAK/CoT
traffic as well, enabling cross-protocol data flow between SAPIENT DLMMs,
TAK clients, and any other mesh-connected translator.

## Prerequisites

- Rust toolchain (stable) with `protoc` installed (required by `peat-schema`
  build script)
- Network access between SAPIENT endpoints and the bridge
- For TLS: PEM-encoded certificates and private keys
- For TAK: a reachable TAK Server endpoint

Build with TLS (default):

```sh
cargo build -p peat-sapient-bridge --release
```

Build without TLS:

```sh
cargo build -p peat-sapient-bridge --release --no-default-features
```

## Configuration

The bridge reads configuration from a TOML file and/or CLI flags. CLI flags
override file values. Pass a config file with `--config`:

```sh
peat-sapient-bridge --config bridge.toml
```

See `bridge.example.toml` for a fully annotated template.

### `[node]` — Identity and storage

| Key       | CLI flag    | Default              | Description                                           |
|-----------|-------------|----------------------|-------------------------------------------------------|
| `name`    | `--name`    | `"sapient-bridge"`   | Friendly name; also the iroh identity seed (same name = same NodeId across restarts) |
| `bind`    | `--bind`    | OS-assigned          | Mesh QUIC endpoint address (`ip:port`)                |
| `storage` | `--storage` | tempdir (ephemeral)  | Persistence directory for Automerge documents         |

When `storage` is omitted the bridge uses a temporary directory — all mesh
state is lost on restart. Set a persistent path for production deployments.

### `[mesh]` — Formation and peering

| Key            | CLI flag | Default                      | Description                                  |
|----------------|----------|------------------------------|----------------------------------------------|
| `formation_id` | —        | `"sapient-bridge-default"`   | Formation identifier for authenticated sync  |
| `shared_key`   | —        | random (per-run)             | Base64-encoded 256-bit shared formation key  |
| `peers`        | `--peer` | `[]`                         | Static peers: `NODE_ID_HEX@ip:port`          |

All bridge instances that should sync must share the same `formation_id` and
`shared_key`. Generate a key with:

```sh
openssl rand -base64 32
```

### `[sapient]` — SAPIENT transport

| Key             | CLI flag              | Default           | Description                                    |
|-----------------|-----------------------|-------------------|------------------------------------------------|
| `role`          | (inferred)            | `"hldmm"`         | `"hldmm"` or `"dlmm"`                         |
| `listen`        | `--sapient-listen`    | `0.0.0.0:12000`   | Listen address (HLDMM mode)                   |
| `remote`        | `--sapient-remote`    | —                 | Remote HLDMM address (DLMM mode, required)    |
| `peer_node_id`  | `--sapient-peer-id`   | `"hldmm-0"`       | Mesh peer ID for the remote HLDMM             |
| `tls`           | `--sapient-tls`       | `false`           | Enable TLS                                     |
| `cert`          | `--sapient-cert`      | —                 | Certificate PEM (server in HLDMM, client in DLMM) |
| `key`           | `--sapient-key`       | —                 | Private key PEM                                |
| `ca_cert`       | `--sapient-ca-cert`   | —                 | CA certificate PEM for peer verification       |
| `tls_server_name` | `--sapient-server-name` | —             | SNI hostname (DLMM mode only)                 |

Setting `--sapient-listen` implies HLDMM mode. Setting `--sapient-remote`
implies DLMM mode. If neither is set, the config file's `role` field is used
(default: HLDMM on `0.0.0.0:12000`).

### `[tak]` — TAK transport (optional)

| Key                | CLI flag                 | Default          | Description                                 |
|--------------------|--------------------------|------------------|---------------------------------------------|
| `server`           | `--tak-server`           | —                | TAK Server address; enables TAK when set    |
| `tls`              | `--tak-tls`              | `false`          | Enable TLS                                  |
| `client_cert`      | `--tak-cert`             | —                | Client certificate PEM for mTLS             |
| `client_key`       | `--tak-key`              | —                | Client private key PEM for mTLS             |
| `ca_cert`          | `--tak-ca-cert`          | —                | CA certificate PEM for server verification  |
| `callsign`         | `--tak-callsign`         | `"Peat-BRIDGE"`  | TAK callsign                                |
| `tls_server_name`  | `--tak-server-name`      | —                | SNI hostname override                       |
| `peer_node_id`     | `--tak-peer-id`          | `"tak-server-0"` | Mesh-side node ID for the TAK Server peer   |
| `max_message_bytes`| `--tak-max-message-bytes`| `65536`          | Maximum inbound CoT XML size in bytes       |

Omit the entire `[tak]` section (and `--tak-server`) to run without TAK.

## Deployment modes

### HLDMM — accept SAPIENT sensors

The bridge listens for DLMM connections, ingests `DetectionReport` and
`StatusReport` messages, and publishes them to the mesh `tracks` collection.

```sh
peat-sapient-bridge --sapient-listen 0.0.0.0:12000
```

This is the default mode. SAPIENT sensors (DLMMs) connect to the bridge, which
acts as their HLDMM. The bridge handles registration handshakes and forwards
detection data into the mesh.

### DLMM — connect to an external HLDMM

The bridge connects to an existing SAPIENT HLDMM and acts as a virtual sensor.
Mesh tracks from other translators (e.g. CoT/TAK) are forwarded upstream as
SAPIENT `DetectionReport` messages.

```sh
peat-sapient-bridge --sapient-remote 10.0.1.5:12000
```

### SAPIENT + TAK bridging

Adding a TAK Server connection enables bidirectional cross-protocol flow.
The `TransportManager` fan-out mechanism routes documents between translators
automatically, with echo-loop prevention.

```sh
peat-sapient-bridge \
  --sapient-listen 0.0.0.0:12000 \
  --tak-server 10.0.0.10:8089 \
  --tak-tls \
  --tak-cert /etc/peat/tak-client.pem \
  --tak-key  /etc/peat/tak-client-key.pem \
  --tak-ca-cert /etc/peat/tak-ca.pem
```

Data flow:

```
SAPIENT DLMMs → bridge (HLDMM) → mesh "tracks" → CotTranslator → TAK Server → ATAK clients
ATAK clients → TAK Server → bridge → mesh "tracks" → SapientTranslator → (DLMM mode only)
```

In HLDMM mode the SAPIENT side is inbound-only (DLMMs push detections up).
In DLMM mode the bridge can also push mesh-originated tracks upstream to
a SAPIENT HLDMM.

## Mesh peering

Bridge instances discover each other via iroh's QUIC transport. On startup
each node prints its Node ID:

```
mesh node 'bridge-uk' ready (id=abc123def456... bind=0.0.0.0:9001)
  reach me with: --peer abc123def456...@10.0.0.1:9001
```

Add this as a static peer on the other instance:

```sh
peat-sapient-bridge --config bridge.toml \
  --peer abc123def456...64hexchars...@10.0.0.1:9001
```

Or in the config file:

```toml
[mesh]
peers = ["abc123def456...@10.0.0.1:9001"]
```

Multiple peers can be specified. The bridge retries connections automatically
with a 5-second backoff.

## TLS setup

### SAPIENT TLS (HLDMM mode)

The bridge acts as the TLS server. Provide a server certificate, key, and
optionally a CA for mutual TLS:

```sh
peat-sapient-bridge \
  --sapient-listen 0.0.0.0:12000 \
  --sapient-tls \
  --sapient-cert /etc/peat/sapient-server.pem \
  --sapient-key  /etc/peat/sapient-server-key.pem \
  --sapient-ca-cert /etc/peat/sapient-ca.pem
```

### SAPIENT TLS (DLMM mode)

The bridge acts as the TLS client. A CA certificate is required for server
verification; client cert/key enable mutual TLS:

```sh
peat-sapient-bridge \
  --sapient-remote 10.0.1.5:12000 \
  --sapient-tls \
  --sapient-cert /etc/peat/sapient-client.pem \
  --sapient-key  /etc/peat/sapient-client-key.pem \
  --sapient-ca-cert /etc/peat/sapient-ca.pem \
  --sapient-server-name hldmm.example.com
```

### TAK mTLS

TAK Server typically requires mutual TLS. All three PEM files are required:

```sh
--tak-tls \
--tak-cert /etc/peat/tak-client.pem \
--tak-key  /etc/peat/tak-client-key.pem \
--tak-ca-cert /etc/peat/tak-ca.pem
```

All certificates must use FIPS-approved algorithms (RSA, ECDSA with P-256/P-384).

## Persistence

Without `--storage`, mesh state lives in a tempdir and is lost on restart.
For durable deployments:

```sh
peat-sapient-bridge --storage /var/lib/peat-sapient-bridge
```

The storage directory contains Automerge documents and iroh blob state. The
`name` field seeds the node identity — using the same name and storage path
across restarts preserves the node's mesh identity and document history.

## Observability

The bridge uses `tracing` with `tracing-subscriber`. Control log levels with
the `RUST_LOG` environment variable:

```sh
# Default filter (applied when RUST_LOG is unset):
# warn,peat_sapient_bridge=info,peat_mesh_sapient=info,peat_sapient=info,peat_tak=info

# Debug mesh sync:
RUST_LOG=debug,peat_mesh=trace peat-sapient-bridge --config bridge.toml

# Quiet mode:
RUST_LOG=error peat-sapient-bridge --config bridge.toml
```

Key log events:
- `mesh node '...' ready` — startup complete, prints Node ID and bind address
- `mesh: connected to peer ...` — peer connection established
- `sapient: started (HLDMM listening on ...)` — SAPIENT transport active
- `tak: started (server=..., tls=...)` — TAK transport active
- `fan-out: observing 'tracks' collection` — cross-protocol routing active

## Troubleshooting

**Bridge starts but no data flows:**
Check that both endpoints share the same `formation_id` and `shared_key`.
Verify mesh peer connectivity — look for `connected to peer` log messages.

**SAPIENT sensor connects but no mesh documents appear:**
Confirm the sensor sends a valid `Registration` first — the bridge requires
a successful registration handshake before processing other messages.

**TAK connection fails with TLS errors:**
Verify all three PEM files (cert, key, CA) are present and the CA chain is
complete. Check `--tak-server-name` matches the server certificate's CN/SAN.

**`binary was compiled without the tls feature`:**
Rebuild with the default feature set (TLS is on by default). Only builds
with `--no-default-features` strip TLS support.

**CoT messages rejected before parsing:**
The `max_message_bytes` limit (default 64 KB) rejects oversized XML at the
TCP boundary. This is an intentional mitigation for quick-xml DoS vectors
(RUSTSEC-2026-0194/0195). Increase the limit only if you trust the source.

**Peer connection retries indefinitely:**
Verify the peer address is reachable and the Node ID is correct (64 hex
characters, matching the peer's printed ID). Check firewall rules for the
QUIC port.
