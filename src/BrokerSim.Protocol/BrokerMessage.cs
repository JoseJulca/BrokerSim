using System.Text.Json;
using System.Text.Json.Serialization;

namespace BrokerSim.Protocol;

/// <summary>
/// Sobre único para todos los mensajes que viajan por el WebSocket,
/// en ambas direcciones. No todos los campos se usan en todos los
/// tipos de mensaje (ver MessageType para el significado de cada uno).
/// </summary>
public class BrokerMessage
{
    [JsonPropertyName("type")]
    public string Type { get; set; } = string.Empty;

    [JsonPropertyName("clientId")]
    public string? ClientId { get; set; }

    [JsonPropertyName("role")]
    public string? Role { get; set; } // "producer" | "consumer"

    [JsonPropertyName("topic")]
    public string? Topic { get; set; }

    [JsonPropertyName("content")]
    public string? Content { get; set; }

    [JsonPropertyName("from")]
    public string? From { get; set; }

    [JsonPropertyName("messageId")]
    public string? MessageId { get; set; }

    [JsonPropertyName("reason")]
    public string? Reason { get; set; }

    [JsonPropertyName("subscriberCount")]
    public int? SubscriberCount { get; set; }

    [JsonPropertyName("timestamp")]
    public DateTime Timestamp { get; set; } = DateTime.UtcNow;

    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
    };

    public string ToJson() => JsonSerializer.Serialize(this, JsonOptions);

    public static BrokerMessage? FromJson(string json) =>
        JsonSerializer.Deserialize<BrokerMessage>(json, JsonOptions);
}
