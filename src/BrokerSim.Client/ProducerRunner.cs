using BrokerSim.Protocol;

namespace BrokerSim.Client;

public static class ProducerRunner
{
    public static async Task RunAsync(BrokerClient client)
    {
        client.OnMessage += msg =>
        {
            switch (msg.Type)
            {
                case MessageType.Registered:
                    Print($"Registrado en el broker con id '{msg.ClientId}' (rol productor)", ConsoleColor.Green);
                    break;
                case MessageType.Ack:
                    Print($"Confirmado por el broker -> mensaje {msg.MessageId} en tópico '{msg.Topic}'", ConsoleColor.DarkGreen);
                    break;
                case MessageType.Error:
                    Print($"Error del broker: {msg.Reason}", ConsoleColor.Red);
                    break;
            }
        };
        client.OnClosed += () => Print("El broker cerró la conexión.", ConsoleColor.Red);

        await client.SendAsync(new BrokerMessage { Type = MessageType.Register, Role = "producer" });

        Console.WriteLine();
        Console.WriteLine("Escribe mensajes como  <topico>: <contenido>   (ej. noticias: hay lluvias en Lima)");
        Console.WriteLine("Escribe 'salir' para terminar.");
        Console.WriteLine();

        while (true)
        {
            Console.Write("> ");
            var line = Console.ReadLine();
            if (line is null || line.Trim().Equals("salir", StringComparison.OrdinalIgnoreCase))
                break;

            var separatorIndex = line.IndexOf(':');
            if (separatorIndex < 1)
            {
                Print("Formato inválido. Usa: <topico>: <mensaje>", ConsoleColor.Yellow);
                continue;
            }

            var topic = line[..separatorIndex].Trim();
            var content = line[(separatorIndex + 1)..].Trim();
            if (topic.Length == 0 || content.Length == 0)
            {
                Print("El tópico y el mensaje no pueden estar vacíos.", ConsoleColor.Yellow);
                continue;
            }

            await client.SendAsync(new BrokerMessage
            {
                Type = MessageType.Publish,
                Topic = topic,
                Content = content
            });
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
