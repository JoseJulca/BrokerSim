using System.Net.WebSockets;
using System.Text;
using BrokerSim.Protocol;

namespace BrokerSim.Client;

/// <summary>
/// Envuelve un ClientWebSocket: conecta, envía mensajes del protocolo y
/// dispara un evento por cada mensaje entrante para que el consumidor
/// de esta clase (Program.cs) decida cómo mostrarlo.
/// </summary>
public class BrokerClient : IAsyncDisposable
{
    private readonly ClientWebSocket _socket = new();
    private CancellationTokenSource? _receiveCts;

    public event Action<BrokerMessage>? OnMessage;
    public event Action? OnClosed;

    public async Task ConnectAsync(string serverUrl)
    {
        await _socket.ConnectAsync(new Uri(serverUrl), CancellationToken.None);
        _receiveCts = new CancellationTokenSource();
        _ = Task.Run(() => ReceiveLoopAsync(_receiveCts.Token));
    }

    public Task SendAsync(BrokerMessage message)
    {
        var bytes = Encoding.UTF8.GetBytes(message.ToJson());
        return _socket.SendAsync(
            new ArraySegment<byte>(bytes),
            WebSocketMessageType.Text,
            endOfMessage: true,
            CancellationToken.None);
    }

    private async Task ReceiveLoopAsync(CancellationToken ct)
    {
        var buffer = new byte[4096];
        try
        {
            while (_socket.State == WebSocketState.Open && !ct.IsCancellationRequested)
            {
                using var ms = new MemoryStream();
                WebSocketReceiveResult result;
                do
                {
                    result = await _socket.ReceiveAsync(new ArraySegment<byte>(buffer), ct);
                    if (result.MessageType == WebSocketMessageType.Close)
                    {
                        OnClosed?.Invoke();
                        return;
                    }
                    ms.Write(buffer, 0, result.Count);
                } while (!result.EndOfMessage);

                var json = Encoding.UTF8.GetString(ms.ToArray());
                var message = BrokerMessage.FromJson(json);
                if (message is not null) OnMessage?.Invoke(message);
            }
        }
        catch (OperationCanceledException)
        {
            // cierre solicitado localmente, no es un error
        }
        catch (WebSocketException)
        {
            OnClosed?.Invoke();
        }
    }

    public async ValueTask DisposeAsync()
    {
        _receiveCts?.Cancel();
        if (_socket.State == WebSocketState.Open)
        {
            try
            {
                await _socket.CloseAsync(WebSocketCloseStatus.NormalClosure, "cierre normal", CancellationToken.None);
            }
            catch { /* el socket ya pudo haberse cerrado del otro lado */ }
        }
        _socket.Dispose();
    }
}
