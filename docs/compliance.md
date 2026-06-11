# BSI Flex 335 v2 Compliance Procedure

This document describes how to run the Dstl BSI Flex 335 v2 test harness against
`peat-sapient` and how to record the results.

This is a **manual gate** — not a CI job. It is analogous to a field-test sign-off
and is required before a Phase 4 completion PR is merged.

**Test harness repository:** https://github.com/dstl/BSI-Flex-335-v2-Test-Harness

---

## Prerequisites

| Requirement | Version | Notes |
|-------------|---------|-------|
| Windows VM or bare-metal | Windows 10/11 or Server 2019+ | The harness is a C# / .NET application |
| .NET SDK | 6.0 | `dotnet --version` must report `6.x.x` |
| PostgreSQL | 12+ | Harness logs results to a local database |
| Rust toolchain | stable | Build `peat-sapient` on the same host or cross-compile |
| Network access | localhost | Harness and `peat-sapient` communicate over TCP on the same host |

---

## Setup

### 1. Clone and build the test harness

```powershell
git clone https://github.com/dstl/BSI-Flex-335-v2-Test-Harness.git
cd BSI-Flex-335-v2-Test-Harness
dotnet build --configuration Release
```

### 2. Configure PostgreSQL

Create the database the harness expects (consult the harness README for the exact
schema creation script — it ships with the repository):

```powershell
psql -U postgres -c "CREATE DATABASE sapient_test;"
# Run the harness migration script
psql -U postgres -d sapient_test -f db/migrations/001_create_tables.sql
```

### 3. Build peat-sapient in DLMM mode

```sh
cargo build --release --features peat
```

No additional binary is shipped yet — Phase 4 (`SapientBridge::start()`) will provide
a runnable bridge binary. Until then, run a minimal harness binary:

```sh
# example invocation once the Phase 4 binary exists:
./target/release/peat-sapient-bridge \
    --mode dlmm \
    --remote 127.0.0.1:5066 \
    --node-id "$(uuidgen)"
```

---

## Running the test harness

The harness acts as the **HLDMM** (manager). `peat-sapient` connects as the **DLMM**.

### 1. Start the harness in HLDMM mode

Follow the harness README for the exact command. Typical invocation:

```powershell
dotnet run --project src/TestHarness \
    --hldmm-port 5066 \
    --output results/run-$(Get-Date -Format "yyyyMMdd-HHmm").json
```

The harness listens on port 5066 (SAPIENT default) and waits for a DLMM to connect.

### 2. Start peat-sapient

Point the bridge at the harness address:

```sh
./target/release/peat-sapient-bridge \
    --mode dlmm \
    --remote 127.0.0.1:5066 \
    --node-id "$(uuidgen)"
```

### 3. Run the test sequence

The harness sends a sequence of messages to the DLMM and validates responses.
The sequence covers all BSI Flex 335 v2.0 message types:

| Step | Harness sends | Expected from peat-sapient |
|------|--------------|---------------------------|
| 1 | RegistrationAck | Registration (sent first by DLMM) |
| 2 | Task | TaskAck with `task_status = Accepted` |
| 3 | Alert (HLDMM→DLMM direction) | AlertAck |
| 4 | Error | (no response required) |

The harness also validates that the DLMM correctly sends:

| DLMM message | Validation |
|-------------|-----------|
| Registration | `node_id` is a valid UUID; `node_definition` is present |
| StatusReport | `system` field set; timestamp is UTC |
| DetectionReport | `report_id` present; location in LatLng or UTM |
| Alert | `alert_id` present; `alert_type` is a known enum value |

---

## Pass / fail criteria

A compliance run **passes** when:

- All harness-initiated message exchanges complete without timeout
- `peat-sapient` sends `TaskAck.task_status = Accepted` for every `Task` received
- No `Error` message is sent by `peat-sapient` during the run
- The harness JSON results file contains no `FAIL` entries

A compliance run **fails** when any of the above conditions are not met, or when
`peat-sapient` drops the connection before the sequence completes.

---

## Recording results

Results must be recorded in the PR description of the Phase 4 completion PR.
Use the following template:

```markdown
## BSI Flex 335 v2 Compliance

| Run date | Harness version | peat-sapient commit | Outcome |
|----------|----------------|---------------------|---------|
| YYYY-MM-DD | vX.Y.Z (git SHA) | abc1234 | PASS / FAIL |

<details>
<summary>Harness output</summary>

```
(paste results JSON or relevant excerpt here)
```

</details>
```

Attach the full results JSON as a PR artifact or paste it in a collapsible block.

---

## PR label

Add the `compliance` label to any PR that triggers a compliance run. This label
is used to track which PRs have been compliance-verified in the repository history.

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---------|-------------|-----|
| Harness times out waiting for Registration | `peat-sapient` not connecting | Check port and `--remote` address |
| `TaskAck` arrives with wrong `task_id` | ULID/UUID mismatch | Verify `to_task()` copies the incoming `task_id` |
| Connection drops immediately | TLS mismatch or codec framing error | Check harness TLS setting; default is plain TCP |
| PostgreSQL connection error | DB not running or schema not created | Start PostgreSQL and run migration script |
