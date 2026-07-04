using BrokerSim.Protocol;

namespace BrokerSim.Client;

public static class ConsumerRunner
{
    public static async Task RunAsync(BrokerClient client)
    {
        client.OnMessage += msg =>
        {
            switch (msg.Type)
            {
                case MessageType.Registered:
                    Print($"Registrado en el broker con id '{msg.ClientId}' (rol consumidor)", ConsoleColor.Green);
                    break;
                case MessageType.Subscribed:
                    Print($"Suscrito al tópico '{msg.Topic}'", ConsoleColor.Blue);
                    break;
                case MessageType.Unsubscribed:
                    Print($"Baja del tópico '{msg.Topic}'", ConsoleColor.DarkBlue);
                    break;
                case MessageType.Deliver:
                    Print($"[{msg.Topic}] de {msg.From}: {msg.Content}", ConsoleColor.Cyan);
                    break;
                case MessageType.Error:
                    Print($"Error del broker: {msg.Reason}", ConsoleColor.Red);
                    break;
            }
        };
        client.OnClosed += () => Print("El broker cerró la conexión.", ConsoleColor.Red);

        await client.SendAsync(new BrokerMessage { Type = MessageType.Register, Role = "consumer" });

        Console.WriteLine();
        Console.WriteLine("Comandos disponibles:");
        Console.WriteLine("  sub <topico>     -> suscribirse a un tópico");
        Console.WriteLine("  unsub <topico>   -> cancelar suscripción");
        Console.WriteLine("  salir            -> terminar");
        Console.WriteLine();

        while (true)
        {
            Console.Write("> ");
            var line = Console.ReadLine();
            if (line is null || line.Trim().Equals("salir", StringComparison.OrdinalIgnoreCase))
                break;

            var parts = line.Trim().Split(' ', 2);
            var command = parts[0].ToLowerInvariant();

            if (command == "sub" && parts.Length == 2)
            {
                await client.SendAsync(new BrokerMessage { Type = MessageType.Subscribe, Topic = parts[1].Trim() });
            }
            else if (command == "unsub" && parts.Length == 2)
            {
                await client.SendAsync(new BrokerMessage { Type = MessageType.Unsubscribe, Topic = parts[1].Trim() });
            }
            else
            {
                Print("Comando no reconocido. Usa 'sub <topico>', 'unsub <topico>' o 'salir'.", ConsoleColor.Yellow);
            }
        }
    }

    private static void Print(string text, ConsoleColor color)
    {
        var prev = Console.ForegroundColor;
        Console.ForegroundColor = color;
        Console.WriteLine($"  {text}");
        Console.ForegroundColor = prev;
    }
}
