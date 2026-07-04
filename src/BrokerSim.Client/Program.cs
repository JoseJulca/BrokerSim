using System.Text;
using BrokerSim.Client;

Console.OutputEncoding = Encoding.UTF8;

Console.WriteLine("=================================================");
Console.WriteLine("  BrokerSim - Cliente de consola");
Console.WriteLine("=================================================");

Console.Write("URL del broker [ws://localhost:5080/ws]: ");
var url = Console.ReadLine();
if (string.IsNullOrWhiteSpace(url)) url = "ws://localhost:5080/ws";

string role;
while (true)
{
    Console.Write("Rol (productor/consumidor): ");
    var input = Console.ReadLine()?.Trim().ToLowerInvariant();
    if (input is "productor" or "producer") { role = "producer"; break; }
    if (input is "consumidor" or "consumer") { role = "consumer"; break; }
    Console.WriteLine("Escribe 'productor' o 'consumidor'.");
}

await using var client = new BrokerClient();

try
{
    await client.ConnectAsync(url);
}
catch (Exception ex)
{
    Console.ForegroundColor = ConsoleColor.Red;
    Console.WriteLine($"No se pudo conectar al broker en '{url}': {ex.Message}");
    Console.ResetColor();
    return;
}

if (role == "producer")
    await ProducerRunner.RunAsync(client);
else
    await ConsumerRunner.RunAsync(client);

Console.WriteLine("Sesión finalizada.");
