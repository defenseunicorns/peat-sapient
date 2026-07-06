// Crown-owned copyright, 2021-2024
namespace SapientComplianceRunner;

using System.Threading.Channels;
using Sapient.Data;
using SapientServices;
using SapientServices.Communication;

/// <summary>
/// Wraps <see cref="SapientServer"/> or <see cref="SapientClient"/> callbacks
/// into an async <see cref="Channel{T}"/> so the compliance runner can await
/// messages instead of polling.
/// </summary>
public sealed class AsyncMessageChannel
{
    private readonly Channel<(SapientMessage Message, uint ConnectionId)> _channel =
        Channel.CreateUnbounded<(SapientMessage, uint)>();

    public ChannelReader<(SapientMessage Message, uint ConnectionId)> Reader => _channel.Reader;

    public void OnDataReceived(SapientMessage msg, IConnection client)
    {
        _channel.Writer.TryWrite((msg, client.ConnectionID));
    }

    public async Task<(SapientMessage Message, uint ConnectionId)> ReceiveAsync(
        TimeSpan timeout, CancellationToken ct = default)
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(timeout);
        try
        {
            return await _channel.Reader.ReadAsync(cts.Token);
        }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested)
        {
            throw new TimeoutException(
                $"No message received within {timeout.TotalSeconds:F0}s");
        }
    }

    public async Task<SapientMessage> ReceiveAsync(
        SapientMessage.ContentOneofCase expectedType,
        TimeSpan timeout,
        CancellationToken ct = default)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            var remaining = deadline - DateTime.UtcNow;
            if (remaining <= TimeSpan.Zero) break;
            var (msg, _) = await ReceiveAsync(remaining, ct);
            if (msg.ContentCase == expectedType)
                return msg;
        }
        throw new TimeoutException(
            $"Did not receive {expectedType} within {timeout.TotalSeconds:F0}s");
    }
}
