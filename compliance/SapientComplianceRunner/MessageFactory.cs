// Crown-owned copyright, 2021-2024
namespace SapientComplianceRunner;

using Google.Protobuf.WellKnownTypes;
using Sapient.Data;

/// <summary>
/// Loads BSI Flex 335 v2.0 sample messages from JSON fixtures and stamps
/// them with fresh identifiers and timestamps for compliance testing.
/// </summary>
public static class MessageFactory
{
    private static readonly string FixturesDir =
        Path.Combine(AppContext.BaseDirectory, "Fixtures");

    public static SapientMessage LoadRegistration(string nodeId, string? destinationId = null)
    {
        var msg = Load("Default.Registration.json");
        msg.NodeId = nodeId;
        msg.DestinationId = destinationId ?? msg.DestinationId;
        msg.Timestamp = Now();
        return msg;
    }

    public static SapientMessage LoadRegistrationAck(
        string hldmmNodeId, string dlmmNodeId, bool accepted = true)
    {
        var msg = Load("Default.RegistrationAck.json");
        msg.NodeId = hldmmNodeId;
        msg.DestinationId = dlmmNodeId;
        msg.Timestamp = Now();
        msg.RegistrationAck.Acceptance = accepted;
        return msg;
    }

    public static SapientMessage LoadStatusReport(string nodeId, string? destinationId = null)
    {
        var msg = Load("Default.StatusReport.json");
        msg.NodeId = nodeId;
        msg.DestinationId = destinationId ?? msg.DestinationId;
        msg.Timestamp = Now();
        if (msg.StatusReport != null)
            msg.StatusReport.ReportId = NewUlid();
        return msg;
    }

    public static SapientMessage LoadDetectionReport(string nodeId, string? destinationId = null)
    {
        var msg = Load("Default.DetectionReport.json");
        msg.NodeId = nodeId;
        msg.DestinationId = destinationId ?? msg.DestinationId;
        msg.Timestamp = Now();
        if (msg.DetectionReport != null)
        {
            msg.DetectionReport.ReportId = NewUlid();
            msg.DetectionReport.ObjectId = NewUlid();
        }
        return msg;
    }

    public static SapientMessage LoadTask(string hldmmNodeId, string dlmmNodeId)
    {
        var msg = Load("Default.Task.LookAt.json");
        msg.NodeId = hldmmNodeId;
        msg.DestinationId = dlmmNodeId;
        msg.Timestamp = Now();
        if (msg.Task != null)
        {
            msg.Task.TaskId = NewUlid();
            msg.Task.TaskStartTime = Now();
            msg.Task.TaskEndTime = Timestamp.FromDateTime(
                DateTime.UtcNow.AddHours(1));
        }
        return msg;
    }

    public static SapientMessage LoadTaskAck(
        string nodeId, string destinationId, string taskId)
    {
        var msg = Load("Default.TaskAck.LookAt.json");
        msg.NodeId = nodeId;
        msg.DestinationId = destinationId;
        msg.Timestamp = Now();
        if (msg.TaskAck != null)
        {
            msg.TaskAck.TaskId = taskId;
            msg.TaskAck.TaskStatus = TaskAck.Types.TaskStatus.Accepted;
        }
        return msg;
    }

    public static SapientMessage LoadAlert(string nodeId, string? destinationId = null)
    {
        var msg = Load("Default.Alert.json");
        msg.NodeId = nodeId;
        msg.DestinationId = destinationId ?? msg.DestinationId;
        msg.Timestamp = Now();
        if (msg.Alert != null)
            msg.Alert.AlertId = NewUlid();
        return msg;
    }

    public static SapientMessage LoadAlertAck(
        string nodeId, string destinationId, string alertId)
    {
        var msg = Load("Default.AlertAck.json");
        msg.NodeId = nodeId;
        msg.DestinationId = destinationId;
        msg.Timestamp = Now();
        if (msg.AlertAck != null)
            msg.AlertAck.AlertId = alertId;
        return msg;
    }

    private static SapientMessage Load(string filename)
    {
        var path = Path.Combine(FixturesDir, filename);
        var json = File.ReadAllText(path);
        return SapientMessage.Parser.ParseJson(json);
    }

    private static Timestamp Now() =>
        Timestamp.FromDateTime(DateTime.UtcNow);

    private static string NewUlid() => Ulid.NewUlid().ToString();
}
