// Crown-owned copyright, 2021-2024
namespace SapientComplianceRunner;

using System.Text.Json;
using System.Text.Json.Serialization;

public enum StepOutcome
{
    Pass,
    Fail,
    Skip,
}

public sealed class StepResult
{
    [JsonPropertyName("step")]
    public required string Step { get; set; }

    [JsonPropertyName("outcome")]
    public StepOutcome Outcome { get; set; }

    [JsonPropertyName("message")]
    public string? Message { get; set; }

    [JsonPropertyName("validationErrors")]
    public List<string>? ValidationErrors { get; set; }

    [JsonPropertyName("durationMs")]
    public long DurationMs { get; set; }
}

public sealed class TestRun
{
    [JsonPropertyName("mode")]
    public required string Mode { get; init; }

    [JsonPropertyName("startTime")]
    public DateTime StartTime { get; init; } = DateTime.UtcNow;

    [JsonPropertyName("steps")]
    public List<StepResult> Steps { get; } = new();

    [JsonPropertyName("passed")]
    public bool Passed => Steps.All(s => s.Outcome != StepOutcome.Fail);

    public string ToJson() => JsonSerializer.Serialize(this, new JsonSerializerOptions
    {
        WriteIndented = true,
        Converters = { new JsonStringEnumConverter() },
    });
}
