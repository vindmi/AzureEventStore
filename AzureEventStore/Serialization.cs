using System;
using Newtonsoft.Json;

namespace AzureEventSourcing
{
    public interface IPayloadSerializer
    {
        SerializedPayload Serialize(object payload);
        object Deserialize(SerializedPayload payload);
    }

    public class DefaultPayloadSerializer : IPayloadSerializer
    {
        public SerializedPayload Serialize(object payload)
        {
            return new SerializedPayload(JsonConvert.SerializeObject(payload), GetTypeName(payload));
        }

        public object Deserialize(SerializedPayload payload)
        {
            return JsonConvert.DeserializeObject(payload.Payload, Type.GetType(payload.Type));
        }

        private static string GetTypeName(object payload)
        {
            var payloadType = payload.GetType();

            return $"{payloadType.FullName}, {payloadType.Assembly.GetName().Name}";
        }
    }

    public struct SerializedPayload
    {
        public SerializedPayload(string payload, string type)
        {
            Payload = payload;
            Type = type;
        }

        public string Payload { get; }
        public string Type { get; }
    }
}
