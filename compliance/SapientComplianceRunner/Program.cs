// Crown-owned copyright, 2021-2024
using System.CommandLine;
using SapientComplianceRunner;

var modeOption = new Option<string>(
    "--mode",
    description: "Role to play: 'hldmm' (test a DLMM) or 'dlmm' (test an HLDMM)")
{
    IsRequired = true,
};
modeOption.AddValidator(result =>
{
    var value = result.GetValueForOption(modeOption);
    if (value != "hldmm" && value != "dlmm")
        result.ErrorMessage = "Mode must be 'hldmm' or 'dlmm'";
});

var hostOption = new Option<string>(
    "--host",
    getDefaultValue: () => "127.0.0.1",
    description: "Listen/connect address");

var portOption = new Option<int>(
    "--port",
    getDefaultValue: () => 12000,
    description: "Listen/connect port");

var timeoutOption = new Option<int>(
    "--timeout",
    getDefaultValue: () => 30,
    description: "Per-step timeout in seconds");

var outputOption = new Option<string?>(
    "--output",
    description: "Write JSON results to this file (stdout if omitted)");

var rootCommand = new RootCommand(
    "BSI Flex 335 v2.0 headless compliance runner. " +
    "Acts as either an HLDMM or DLMM to validate the peer implementation.")
{
    modeOption,
    hostOption,
    portOption,
    timeoutOption,
    outputOption,
};

rootCommand.SetHandler(async (string mode, string host, int port, int timeout, string? output) =>
{
    var cts = new CancellationTokenSource();
    Console.CancelKeyPress += (_, e) =>
    {
        e.Cancel = true;
        cts.Cancel();
    };

    var timespan = TimeSpan.FromSeconds(timeout);

    Console.WriteLine($"BSI Flex 335 v2.0 Compliance Runner — mode={mode}, {host}:{port}, timeout={timeout}s");
    Console.WriteLine();

    TestRun result;
    if (mode == "hldmm")
    {
        var scenario = new HldmmScenario(host, port, timespan);
        result = await scenario.RunAsync(cts.Token);
    }
    else
    {
        var scenario = new DlmmScenario(host, port, timespan);
        result = await scenario.RunAsync(cts.Token);
    }

    Console.WriteLine();
    Console.WriteLine($"Result: {(result.Passed ? "PASS" : "FAIL")}");
    Console.WriteLine($"  Steps: {result.Steps.Count(s => s.Outcome == StepOutcome.Pass)} passed, " +
                      $"{result.Steps.Count(s => s.Outcome == StepOutcome.Fail)} failed, " +
                      $"{result.Steps.Count(s => s.Outcome == StepOutcome.Skip)} skipped");

    var json = result.ToJson();

    if (output != null)
    {
        await File.WriteAllTextAsync(output, json);
        Console.WriteLine($"  Results written to {output}");
    }
    else
    {
        Console.WriteLine();
        Console.WriteLine(json);
    }

    Environment.ExitCode = result.Passed ? 0 : 1;
}, modeOption, hostOption, portOption, timeoutOption, outputOption);

return await rootCommand.InvokeAsync(args);
