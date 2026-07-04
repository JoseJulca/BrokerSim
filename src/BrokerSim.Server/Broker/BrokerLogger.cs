namespace BrokerSim.Server.Broker;

/// <summary>
/// Logging de consola centralizado para los eventos que pide la actividad:
/// cliente conectado, cliente desconectado, mensaje recibido, mensaje distribuido.
/// </summary>
public static class BrokerLogger
{
    private static void Write(string tag, ConsoleColor color, string message)
    {
        var prevColor = Console.ForegroundColor;
        Console.ForegroundColor = color;
        Console.Write($"[{DateTime.Now:HH:mm:ss}] {tag} ");
        Console.ForegroundColor = prevColor;
        Console.WriteLine(message);
    }

    public static void ClientConnected(string clientId, string role) =>
        Write("[CONECTADO]  ", ConsoleColor.Green, $"Cliente '{clientId}' conectado como {role}");

    public static void ClientDisconnected(string clientId) =>
        Write("[DESCONECTADO]", ConsoleColor.DarkYellow, $"Cliente '{clientId}' se desconectó");

    public static void MessageReceived(string clientId, string topic, string content) =>
        Write("[RECIBIDO]   ", ConsoleColor.Cyan, $"'{clientId}' publicó en tópico '{topic}': \"{content}\"");

    public static void MessageDelivered(string topic, int subscriberCount, string messageId) =>
        Write("[DISTRIBUIDO]", ConsoleColor.Magenta, $"Mensaje {messageId} del tópico '{topic}' entregado a {subscriberCount} suscriptor(es)");

    public static void Subscribed(string clientId, string topic) =>
        Write("[SUSCRITO]   ", ConsoleColor.Blue, $"Cliente '{clientId}' se suscribió al tópico '{topic}'");

    public static void Unsubscribed(string clientId, string topic) =>
        Write("[DESUSCRITO] ", ConsoleColor.DarkBlue, $"Cliente '{clientId}' canceló su suscripción al tópico '{topic}'");

    public static void Error(string clientId, string reason) =>
        Write("[ERROR]      ", ConsoleColor.Red, $"Cliente '{clientId}': {reason}");

    public static void Info(string message) =>
        Write("[INFO]       ", ConsoleColor.Gray, message);
}
