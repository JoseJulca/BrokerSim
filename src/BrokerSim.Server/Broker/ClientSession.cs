using System.Collections.Concurrent;
using System.Net.WebSockets;
using System.Text;
using BrokerSim.Protocol;

namespace BrokerSim.Server.Broker;

/// <summary>
/// Representa a un cliente conectado al broker: su WebSocket, su rol
/// (producer/consumer) y las suscripciones activas. Un SemaphoreSlim
/// protege el socket porque WebSocket no admite escrituras concurrentes
/// desde varios hilos a la vez.
/// </summary>
public class ClientSession
{
    public string ClientId { get; }
    public WebSocket Socket { get; }
    public string Role { get; set; } = "unknown";
    public ConcurrentDictionary<string, byte> Topics { get; } = new();

    private readonly SemaphoreSlim _sendLock = new(1, 1);

    public ClientSession(string clientId, WebSocket socket)
    {
        ClientId = clientId;
        Socket = socket;
    }

    public async Task SendAsync(BrokerMessage message, CancellationToken ct = default)
    {
        if (Socket.State != WebSocketState.Open) return;

        var json = message.ToJson();
        var bytes = Encoding.UTF8.GetBytes(json);

        await _sendLock.WaitAsync(ct);
        try
        {
            if (Socket.State != WebSocketState.Open) return;
            await Socket.SendAsync(
                new ArraySegment<byte>(bytes),
                WebSocketMessageType.Text,
                endOfMessage: true,
                ct);
        }
        finally
        {
            _sendLock.Release();
        }
    }
}
