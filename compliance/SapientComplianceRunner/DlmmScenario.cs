// Crown-owned copyright, 2021-2024
namespace SapientComplianceRunner;

using System.Diagnostics;
using FluentValidation.Results;
using Sapient.Data;
using SapientServices.Data.Validation;
using SapientServices;
using Task = System.Threading.Tasks.Task;

/// <summary>
/// Acts as a DLMM (sensor-side relay) to validate an HLDMM implementation.
/// Connects to the HLDMM, sends the standard message sequence, and validates
/// every response received.
/// </summary>
public sealed class DlmmScenario
{
    private readonly string _remoteHost;
    private readonly int _remotePort;
    private readonly TimeSpan _timeout;
    private readonly SapientMainMessageValidator _validator = new();
    private readonly string _dlmmNodeId = Guid.NewGuid().ToString();

    public DlmmScenario(string remoteHost, int remotePort, TimeSpan timeout)
    {
        _remoteHost = remoteHost;
        _remotePort = remotePort;
        _timeout = timeout;
    }

    public async Task<TestRun> RunAsync(CancellationToken ct = default)
    {
        var run = new TestRun { Mode = "dlmm" };
        var channel = new AsyncMessageChannel();

        var client = new SapientClient(_remoteHost, _remotePort);
        client.SetDataReceivedCallback(channel.OnDataReceived);
        client.Start();

        Console.WriteLine($"DLMM connecting to {_remoteHost}:{_remotePort}...");

        // Give the background thread time to connect.
        await Task.Delay(2000, ct);

        if (!client.IsConnected())
        {
            run.Steps.Add(new StepResult
            {
                Step = "Connect",
                Outcome = StepOutcome.Fail,
                Message = $"Failed to connect to {_remoteHost}:{_remotePort}",
            });
            return run;
        }

        run.Steps.Add(new StepResult
        {
            Step = "Connect",
            Outcome = StepOutcome.Pass,
            Message = $"Connected to {_remoteHost}:{_remotePort}",
        });

        string? hldmmNodeId = null;

        try
        {
            // Step 1: Send Registration, receive RegistrationAck
            await RunStep(run, "Registration → RegistrationAck", async () =>
            {
                var reg = MessageFactory.LoadRegistration(_dlmmNodeId);
                var sendResult = Validate(reg);
                if (sendResult.Outcome != StepOutcome.Pass)
                    return sendResult;

                client.SendMessage(reg);

                var ackMsg = await channel.ReceiveAsync(
                    SapientMessage.ContentOneofCase.RegistrationAck, _timeout, ct);
                hldmmNodeId = ackMsg.NodeId;
                var result = Validate(ackMsg);

                if (ackMsg.RegistrationAck?.Acceptance != true)
                {
                    result.Outcome = StepOutcome.Fail;
                    result.Message = "RegistrationAck.acceptance = false";
                }

                return result;
            });

            // Step 2: Send StatusReport
            await RunStep(run, "Send StatusReport", () =>
            {
                var msg = MessageFactory.LoadStatusReport(_dlmmNodeId, hldmmNodeId);
                var result = Validate(msg);
                if (result.Outcome == StepOutcome.Pass)
                    client.SendMessage(msg);
                return Task.FromResult(result);
            });

            // Step 3: Send DetectionReport
            await RunStep(run, "Send DetectionReport", () =>
            {
                var msg = MessageFactory.LoadDetectionReport(_dlmmNodeId, hldmmNodeId);
                var result = Validate(msg);
                if (result.Outcome == StepOutcome.Pass)
                    client.SendMessage(msg);
                return Task.FromResult(result);
            });

            // Step 4: Send Alert
            await RunStep(run, "Send Alert", () =>
            {
                var msg = MessageFactory.LoadAlert(_dlmmNodeId, hldmmNodeId);
                var result = Validate(msg);
                if (result.Outcome == StepOutcome.Pass)
                    client.SendMessage(msg);
                return Task.FromResult(result);
            });

            // Step 5: Receive Task, send TaskAck
            await RunStep(run, "Task → TaskAck round-trip", async () =>
            {
                try
                {
                    var taskMsg = await channel.ReceiveAsync(
                        SapientMessage.ContentOneofCase.Task, _timeout, ct);
                    var result = Validate(taskMsg);

                    var ack = MessageFactory.LoadTaskAck(
                        _dlmmNodeId,
                        taskMsg.NodeId,
                        taskMsg.Task?.TaskId ?? "");
                    client.SendMessage(ack);

                    return result;
                }
                catch (TimeoutException)
                {
                    return new StepResult
                    {
                        Step = "Task → TaskAck",
                        Outcome = StepOutcome.Skip,
                        Message = "HLDMM did not send a Task within the timeout",
                    };
                }
            });

            // Step 6: Check for Error messages (should be none)
            await RunStep(run, "No Error messages", async () =>
            {
                try
                {
                    await channel.ReceiveAsync(
                        SapientMessage.ContentOneofCase.Error,
                        TimeSpan.FromSeconds(2), ct);
                    return new StepResult
                    {
                        Step = "No Error messages",
                        Outcome = StepOutcome.Fail,
                        Message = "HLDMM sent an Error message",
                    };
                }
                catch (TimeoutException)
                {
                    return new StepResult
                    {
                        Step = "No Error messages",
                        Outcome = StepOutcome.Pass,
                        Message = "No Error messages received (expected)",
                    };
                }
            });
        }
        finally
        {
            client.Shutdown();
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
