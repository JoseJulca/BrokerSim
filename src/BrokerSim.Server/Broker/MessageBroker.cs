using System.Collections.Concurrent;
using System.Net.WebSockets;
using System.Text;
using BrokerSim.Protocol;

namespace BrokerSim.Server.Broker;

/// <summary>
/// Broker de mensajes en memoria basado en Publish/Subscribe por tópicos.
/// Registrado como singleton en el contenedor de DI: una sola instancia
/// para toda la vida del proceso, compartida por todas las conexiones.
///
/// Estructuras thread-safe porque cada cliente conectado corre en su
/// propio Task de recepción (uno por cada WebSocket aceptado).
/// </summary>
public class MessageBroker
{
    // clientId -> sesión del cliente
    private readonly ConcurrentDictionary<string, ClientSession> _clients = new();

    // topic -> conjunto de clientIds suscritos (ConcurrentDictionary usado como set)
    private readonly ConcurrentDictionary<string, ConcurrentDictionary<string, byte>> _subscriptions = new();

    private const int ReceiveBufferSize = 4096;

    /// <summary>
    /// Punto de entrada: acepta un WebSocket ya aceptado por el middleware
    /// y corre el ciclo de vida completo de esa conexión hasta que se cierre.
    /// </summary>
    public async Task HandleConnectionAsync(WebSocket socket, CancellationToken appLifetime)
    {
        var clientId = Guid.NewGuid().ToString("N")[..8];
        var session = new ClientSession(clientId, socket);

        try
        {
            await ReceiveLoopAsync(session, appLifetime);
        }
        finally
        {
            Unregister(session);
        }
    }

    private async Task ReceiveLoopAsync(ClientSession session, CancellationToken appLifetime)
    {
        var buffer = new byte[ReceiveBufferSize];

        while (session.Socket.State == WebSocketState.Open && !appLifetime.IsCancellationRequested)
        {
            string? json;
            try
            {
                json = await ReceiveFullMessageAsync(session.Socket, buffer, appLifetime);
            }
            catch (WebSocketException)
            {
                break; // conexión cortada abruptamente por el cliente
            }
            catch (OperationCanceledException)
            {
                break; // el servidor se está apagando
            }

            if (json is null) break; // el cliente cerró la conexión normalmente

            BrokerMessage? message;
            try
            {
                message = BrokerMessage.FromJson(json);
            }
            catch (Exception)
            {
                await TrySendErrorAsync(session, "Mensaje con formato JSON inválido");
                continue;
            }

            if (message is null) continue;

            await DispatchAsync(session, message);
        }
    }

    private static async Task<string?> ReceiveFullMessageAsync(WebSocket socket, byte[] buffer, CancellationToken ct)
    {
        using var ms = new MemoryStream();
        WebSocketReceiveResult result;
        do
        {
            result = await socket.ReceiveAsync(new ArraySegment<byte>(buffer), ct);
            if (result.MessageType == WebSocketMessageType.Close)
            {
                await socket.CloseAsync(WebSocketCloseStatus.NormalClosure, "Cierre solicitado por el cliente", ct);
                return null;
            }
            ms.Write(buffer, 0, result.Count);
        } while (!result.EndOfMessage);

        return Encoding.UTF8.GetString(ms.ToArray());
    }

    private async Task DispatchAsync(ClientSession session, BrokerMessage message)
    {
        switch (message.Type)
        {
            case MessageType.Register:
                await HandleRegisterAsync(session, message);
                break;

            case MessageType.Subscribe:
                await HandleSubscribeAsync(session, message);
                break;

            case MessageType.Unsubscribe:
                await HandleUnsubscribeAsync(session, message);
                break;

            case MessageType.Publish:
                await HandlePublishAsync(session, message);
                break;

            default:
                await TrySendErrorAsync(session, $"Tipo de mensaje no reconocido: '{message.Type}'");
                break;
        }
    }

    private Task HandleRegisterAsync(ClientSession session, BrokerMessage message)
    {
        session.Role = message.Role is "producer" or "consumer" ? message.Role! : "consumer";
        _clients[session.ClientId] = session;

        BrokerLogger.ClientConnected(session.ClientId, session.Role);

        return session.SendAsync(new BrokerMessage
        {
            Type = MessageType.Registered,
            ClientId = session.ClientId,
            Role = session.Role
        });
    }

    private Task HandleSubscribeAsync(ClientSession session, BrokerMessage message)
    {
        if (string.IsNullOrWhiteSpace(message.Topic))
            return TrySendErrorAsync(session, "Debes indicar un tópico para suscribirte");

        var topic = message.Topic.Trim();
        session.Topics[topic] = 1;
        _subscriptions.GetOrAdd(topic, _ => new ConcurrentDictionary<string, byte>())[session.ClientId] = 1;

        BrokerLogger.Subscribed(session.ClientId, topic);

        return session.SendAsync(new BrokerMessage
        {
            Type = MessageType.Subscribed,
            Topic = topic,
            ClientId = session.ClientId
        });
    }

    private Task HandleUnsubscribeAsync(ClientSession session, BrokerMessage message)
    {
        if (string.IsNullOrWhiteSpace(message.Topic))
            return TrySendErrorAsync(session, "Debes indicar un tópico para desuscribirte");

        var topic = message.Topic.Trim();
        session.Topics.TryRemove(topic, out _);
        if (_subscriptions.TryGetValue(topic, out var subs))
            subs.TryRemove(session.ClientId, out _);

        BrokerLogger.Unsubscribed(session.ClientId, topic);

        return session.SendAsync(new BrokerMessage
        {
            Type = MessageType.Unsubscribed,
            Topic = topic,
            ClientId = session.ClientId
        });
    }

    private async Task HandlePublishAsync(ClientSession session, BrokerMessage message)
    {
        if (string.IsNullOrWhiteSpace(message.Topic) || string.IsNullOrWhiteSpace(message.Content))
        {
            await TrySendErrorAsync(session, "Un mensaje publicado requiere 'topic' y 'content'");
            return;
        }

        var topic = message.Topic.Trim();
        var messageId = Guid.NewGuid().ToString("N")[..6];

        BrokerLogger.MessageReceived(session.ClientId, topic, message.Content);

        // Confirmación al productor (requisito: "confirmar el envío del mensaje")
        await session.SendAsync(new BrokerMessage
        {
            Type = MessageType.Ack,
            MessageId = messageId,
            Topic = topic
        });

        if (!_subscriptions.TryGetValue(topic, out var subscriberIds) || subscriberIds.IsEmpty)
        {
            BrokerLogger.MessageDelivered(topic, 0, messageId);
            return;
        }

        var deliverMessage = new BrokerMessage
        {
            Type = MessageType.Deliver,
            Topic = topic,
            Content = message.Content,
            From = session.ClientId,
            MessageId = messageId
        };

        var deliveries = new List<Task>();
        foreach (var subscriberId in subscriberIds.Keys)
        {
            if (_clients.TryGetValue(subscriberId, out var subscriberSession))
                deliveries.Add(subscriberSession.SendAsync(deliverMessage));
        }

        await Task.WhenAll(deliveries);
        BrokerLogger.MessageDelivered(topic, deliveries.Count, messageId);
    }

    private void Unregister(ClientSession session)
    {
        _clients.TryRemove(session.ClientId, out _);
        foreach (var topic in session.Topics.Keys)
        {
            if (_subscriptions.TryGetValue(topic, out var subs))
                subs.TryRemove(session.ClientId, out _);
        }

        BrokerLogger.ClientDisconnected(session.ClientId);
    }

    private static Task TrySendErrorAsync(ClientSession session, string reason)
    {
        BrokerLogger.Error(session.ClientId, reason);
        return session.SendAsync(new BrokerMessage
        {
            Type = MessageType.Error,
            Reason = reason
        });
    }
}
