# SapientComplianceRunner

Headless, cross-platform BSI Flex 335 v2.0 compliance test runner.
Validates SAPIENT implementations without a GUI — suitable for CI pipelines
and automated testing on Linux, macOS, and Windows.

## Requirements

- .NET 8.0 SDK

## Quick start

```bash
# Build
dotnet build SapientComplianceRunner/SapientComplianceRunner.csproj

# Run as HLDMM (test a DLMM implementation)
dotnet run --project SapientComplianceRunner -- --mode hldmm --port 12000

# Run as DLMM (test an HLDMM implementation)
dotnet run --project SapientComplianceRunner -- --mode dlmm --host 127.0.0.1 --port 12000
```

## Modes

### HLDMM mode (`--mode hldmm`)

Acts as a Decision Making Module (HLDMM). Listens for a DLMM to connect
and exercises the full BSI Flex 335 v2.0 message exchange:

1. Receives `Registration` from the DLMM and validates it
2. Sends `RegistrationAck` back
3. Receives `StatusReport` and validates it
4. Receives `DetectionReport` and validates it
5. Sends a `Task` and expects `TaskAck` in return
6. Optionally sends an `Alert` and checks for `AlertAck`

### DLMM mode (`--mode dlmm`)

Acts as a sensor-side relay (DLMM). Connects to the HLDMM and sends the
standard message sequence:

1. Sends `Registration`, expects `RegistrationAck`
2. Sends `StatusReport`
3. Sends `DetectionReport`
4. Sends `Alert`
5. Receives `Task`, sends `TaskAck`
6. Checks for `Error` messages (expects none)

## Options

| Option | Default | Description |
|--------|---------|-------------|
| `--mode` | *(required)* | `hldmm` or `dlmm` |
| `--host` | `127.0.0.1` | Listen/connect address |
| `--port` | `12000` | Listen/connect port |
| `--timeout` | `30` | Per-step timeout in seconds |
| `--output` | *(stdout)* | Write JSON results to a file |

## Output

Results are emitted as structured JSON. The process exits with code 0 on
pass, 1 on failure. Each step includes validation results from the BSI Flex
335 v2.0 FluentValidation validators in `SapientServices`.

```json
{
  "mode": "hldmm",
  "startTime": "2026-07-06T12:00:00Z",
  "steps": [
    {
      "step": "Receive Registration",
      "outcome": "Pass",
      "durationMs": 42
    }
  ],
  "passed": true
}
```

## CI usage

```yaml
# GitHub Actions example
- uses: actions/setup-dotnet@v4
  with:
    dotnet-version: '8.0'
- run: dotnet build SapientComplianceRunner/SapientComplianceRunner.csproj
- run: |
    # Start the system under test in the background
    ./your-sapient-dlmm --remote 127.0.0.1:15066 &
    # Run the compliance suite
    dotnet run --project SapientComplianceRunner -- \
      --mode hldmm --port 15066 --timeout 30 \
      --output results.json
```

## Architecture

The runner reuses the existing cross-platform libraries from this repository:

- **SapientServices** — protobuf types, TCP client/server, FluentValidation
  message validators
- **SAPIENTMessageProcessor** — wire framing (4-byte LE length prefix +
  protobuf)

No WinForms or Windows-specific dependencies. Runs anywhere .NET 8 runs.
