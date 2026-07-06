// Crown-owned copyright, 2021-2024
namespace SapientComplianceRunner;

using System.Diagnostics;
using FluentValidation.Results;
using Sapient.Data;
using SapientServices.Data.Validation;
using SapientServices.Communication;
using Task = System.Threading.Tasks.Task;

/// <summary>
/// Acts as an HLDMM (Decision Making Module) to validate a DLMM implementation.
/// Listens for a DLMM connection, exercises the full BSI Flex 335 v2.0 message
/// exchange, and validates every message received.
/// </summary>
public sealed class HldmmScenario
{
    private readonly string _listenAddress;
    private readonly int _port;
    private readonly TimeSpan _timeout;
    private readonly SapientMainMessageValidator _validator = new();
    private readonly string _hldmmNodeId = Guid.NewGuid().ToString();

    public HldmmScenario(string listenAddress, int port, TimeSpan timeout)
    {
        _listenAddress = listenAddress;
        _port = port;
        _timeout = timeout;
    }

    public async Task<TestRun> RunAsync(CancellationToken ct = default)
    {
        var run = new TestRun { Mode = "hldmm" };
        var channel = new AsyncMessageChannel();

        var server = new SapientServer(_listenAddress, _port, validationEnabled: false);
        server.SetDataReceivedCallback(channel.OnDataReceived);
        server.Start();

        Console.WriteLine($"HLDMM listening on {_listenAddress}:{_port}, waiting for DLMM...");

        try
        {
            // Step 1: Receive Registration
            string? dlmmNodeId = null;
            uint dlmmConnectionId = 0;
            await RunStep(run, "Receive Registration", async () =>
            {
                var (msg, connId) = await channel.ReceiveAsync(_timeout, ct);
                if (msg.ContentCase != SapientMessage.ContentOneofCase.Registration)
                    return Fail($"Expected Registration, got {msg.ContentCase}");

                dlmmNodeId = msg.NodeId;
                dlmmConnectionId = connId;
                return Validate(msg);
            });

            if (dlmmNodeId == null)
            {
                run.Steps.Add(new StepResult
                {
                    Step = "Remaining steps",
                    Outcome = StepOutcome.Skip,
                    Message = "No Registration received; cannot continue",
                });
                return run;
            }

            // Step 2: Send RegistrationAck
            await RunStep(run, "Send RegistrationAck", () =>
            {
                var ack = MessageFactory.LoadRegistrationAck(_hldmmNodeId, dlmmNodeId);
                var result = Validate(ack);
                if (result.Outcome == StepOutcome.Pass)
                    server.SendMessage(ack, dlmmConnectionId);
                return Task.FromResult(result);
            });

            // Step 3: Receive StatusReport
            await RunStep(run, "Receive StatusReport", async () =>
            {
                var msg = await channel.ReceiveAsync(
                    SapientMessage.ContentOneofCase.StatusReport, _timeout, ct);
                return Validate(msg);
            });

            // Step 4: Receive DetectionReport
            await RunStep(run, "Receive DetectionReport", async () =>
            {
                var msg = await channel.ReceiveAsync(
                    SapientMessage.ContentOneofCase.DetectionReport, _timeout, ct);
                return Validate(msg);
            });

            // Step 5: Send Task, receive TaskAck
            await RunStep(run, "Task → TaskAck round-trip", async () =>
            {
                var task = MessageFactory.LoadTask(_hldmmNodeId, dlmmNodeId);
                var sendResult = Validate(task);
                if (sendResult.Outcome != StepOutcome.Pass)
                    return sendResult;

                server.SendMessage(task, dlmmConnectionId);

                var ackMsg = await channel.ReceiveAsync(
                    SapientMessage.ContentOneofCase.TaskAck, _timeout, ct);
                var result = Validate(ackMsg);

                if (ackMsg.TaskAck?.TaskId != task.Task?.TaskId)
                {
                    result.Outcome = StepOutcome.Fail;
                    result.Message = $"TaskAck.task_id mismatch: expected {task.Task?.TaskId}, got {ackMsg.TaskAck?.TaskId}";
                }

                return result;
            });

            // Step 6: Send Alert (optional — DLMM may not support AlertAck)
            await RunStep(run, "Alert → AlertAck round-trip", async () =>
            {
                var alert = MessageFactory.LoadAlert(_hldmmNodeId, dlmmNodeId);
                server.SendMessage(alert, dlmmConnectionId);

                try
                {
                    var ackMsg = await channel.ReceiveAsync(
                        SapientMessage.ContentOneofCase.AlertAck,
                        TimeSpan.FromSeconds(5), ct);
                    return Validate(ackMsg);
                }
                catch (TimeoutException)
                {
                    return new StepResult
                    {
                        Step = "Alert → AlertAck round-trip",
                        Outcome = StepOutcome.Skip,
                        Message = "DLMM did not send AlertAck (optional per BSI Flex 335 v2.0)",
                    };
                }
            });
        }
        finally
        {
            server.Shutdown();
        }

        return run;
    }

    private StepResult Validate(SapientMessage msg)
    {
        ValidationResult validation = _validator.Validate(msg);
        var result = new StepResult
        {
            Step = msg.ContentCase.ToString(),
            Outcome = validation.IsValid ? StepOutcome.Pass : StepOutcome.Fail,
        };
        if (!validation.IsValid)
        {
            result.ValidationErrors = validation.Errors
                .Select(e => $"{e.PropertyName}: {e.ErrorMessage}")
                .ToList();
            result.Message = $"{validation.Errors.Count} validation error(s)";
        }
        return result;
    }

    private static StepResult Fail(string message) => new()
    {
        Step = "?",
        Outcome = StepOutcome.Fail,
        Message = message,
    };

    private static async Task RunStep(
        TestRun run, string name, Func<Task<StepResult>> action)
    {
        var sw = Stopwatch.StartNew();
        StepResult result;
        try
        {
            result = await action();
        }
        catch (TimeoutException ex)
        {
            result = new StepResult
            {
                Step = name,
                Outcome = StepOutcome.Fail,
                Message = ex.Message,
            };
        }
        catch (Exception ex)
        {
            result = new StepResult
            {
                Step = name,
                Outcome = StepOutcome.Fail,
                Message = ex.ToString(),
            };
        }

        result.Step = name;
        result.DurationMs = sw.ElapsedMilliseconds;
        run.Steps.Add(result);

        var symbol = result.Outcome switch
        {
            StepOutcome.Pass => "PASS",
            StepOutcome.Fail => "FAIL",
            StepOutcome.Skip => "SKIP",
            _ => "????",
        };
        Console.WriteLine($"  [{symbol}] {name}{(result.Message != null ? $" — {result.Message}" : "")}");
    }
}
