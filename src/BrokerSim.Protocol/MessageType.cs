namespace BrokerSim.Protocol;

/// <summary>
/// Tipos de mensaje del protocolo WebSocket propio del broker.
/// Se usan como strings (no enum serializado) para evitar problemas de
/// casing entre cliente y servidor y para que el JSON sea legible al
/// inspeccionarlo en herramientas como el navegador o Wireshark.
/// </summary>
public static class MessageType
{
    // Cliente -> Servidor
    public const string Register = "register";
    public const string Subscribe = "subscribe";
    public const string Unsubscribe = "unsubscribe";
    public const string Publish = "publish";

    // Servidor -> Cliente
    public const string Registered = "registered";
    public const string Subscribed = "subscribed";
    public const string Unsubscribed = "unsubscribed";
    public const string Ack = "ack";
    public const string Deliver = "deliver";
    public const string Error = "error";
}
