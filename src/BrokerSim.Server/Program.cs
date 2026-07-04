using System.Net.WebSockets;
using BrokerSim.Server.Broker;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddSingleton<MessageBroker>();

var app = builder.Build();

app.UseDefaultFiles();   // sirve wwwroot/index.html en "/"
app.UseStaticFiles();    // sirve el resto de wwwroot (css/js si se agregan)

app.UseWebSockets(new WebSocketOptions
{
    KeepAliveInterval = TimeSpan.FromSeconds(30)
});

app.Map("/ws", async (HttpContext context, MessageBroker broker) =>
{
    if (!context.WebSockets.IsWebSocketRequest)
    {
        context.Response.StatusCode = StatusCodes.Status400BadRequest;
        await context.Response.WriteAsync("Este endpoint solo acepta conexiones WebSocket.");
        return;
    }

    using var socket = await context.WebSockets.AcceptWebSocketAsync();
    await broker.HandleConnectionAsync(socket, context.RequestAborted);
});

Console.ForegroundColor = ConsoleColor.White;
Console.WriteLine("=================================================");
Console.WriteLine("  BrokerSim - Servidor Broker (Pub/Sub por tópicos)");
Console.WriteLine("  Endpoint WebSocket: ws://localhost:5080/ws");
Console.WriteLine("  Cliente web:        http://localhost:5080/");
Console.WriteLine("=================================================");
Console.ResetColor();

app.Run();
