using System.Diagnostics.Contracts;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace AzureEventSourcing
{
    public class MessageStore
    {
        private readonly IPayloadSerializer serializer;
        private readonly AzureTableStore<MessageEntity> messageStore;

        public MessageStore(CloudStorageAccount storageAccount, string tableName, IPayloadSerializer serializer = null)
        {
            Contract.Requires(storageAccount != null);
            Contract.Requires(!string.IsNullOrWhiteSpace(tableName));

            this.serializer = serializer ?? new DefaultPayloadSerializer();
            messageStore = new AzureTableStore<MessageEntity>(storageAccount, tableName);
        }

        public bool Register(string messageId, object message)
        {
            var messageName = message.GetType()
                .FullName;

            var entityId = new EntityId(messageId, messageName);

            var incomingMessage = messageStore.GetSingle(entityId);

            if (incomingMessage != null)
            {
                return false;
            }

            var serialized = serializer.Serialize(message);

            incomingMessage = new MessageEntity
            {
                Id = messageId,
                Type = messageName,
                Payload = serialized.Payload,
                RowKey = entityId.RowKey,
                PartitionKey = entityId.ParitionKey
            };

            messageStore.Insert(incomingMessage);

            return true;
        }
    }

    internal class MessageEntity : TableEntity
    {
        public string Id { get; set; }
        public string Type { get; set; }
        public string Payload { get; set; }
    }
}