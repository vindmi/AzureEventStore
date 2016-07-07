using System;
using System.Collections.Generic;
using System.Diagnostics.Contracts;
using System.Linq;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace AzureEventSourcing
{
    public class EventStore
    {
        private readonly IPayloadSerializer serializer;
        private readonly AzureTableStore<EventEntity> eventStore;

        public EventStore(CloudStorageAccount storageAccount, string tableName, IPayloadSerializer serializer = null)
        {
            Contract.Requires(storageAccount != null);
            Contract.Requires(!string.IsNullOrWhiteSpace(tableName));

            this.serializer = serializer ?? new DefaultPayloadSerializer();
            eventStore = new AzureTableStore<EventEntity>(storageAccount, tableName);
        }

        public void SetSynchronized(string eventId, EventSource eventSource, DateTimeOffset syncTime)
        {
            var id = new EntityId(eventId, GetEventPartitionKey(eventSource));

            var entity = eventStore.GetSingle(id);

            if (entity == null)
            {
                throw new AzureTableStoreException("Entity not found");
            }

            entity.SynchronizedAt = syncTime;

            eventStore.Update(entity);

            entity.PartitionKey = GetMessagePartitionKey(entity.MessageId);

            eventStore.Update(entity);
        }

        public IEnumerable<Event> GetStream(EventSource eventSource)
        {
            return eventStore.GetPartition(GetEventPartitionKey(eventSource))
                .OrderBy(o => o.Timestamp)
                .Select(e => new Event(e.RowKey, eventSource, e.MessageId, serializer.Deserialize(new SerializedPayload(e.Payload, e.Type))))
                .ToArray();
        }

        public IEnumerable<Event> GetStream(string messageId)
        {
            return eventStore.GetPartition(GetMessagePartitionKey(messageId))
                .OrderBy(o => o.Timestamp)
                .Select(e =>
                {
                    var deserialized = serializer.Deserialize(new SerializedPayload(e.Payload, e.Type));

                    return new Event(e.RowKey, new EventSource(e.StreamName, e.AggregateId), messageId, deserialized);
                })
                .ToArray();
        }

        public void Commit(IEnumerable<Event> events)
        {
            var eventEntities = events
                .Select(e =>
                {
                    var serializedPayload = serializer.Serialize(e.Payload);

                    return new EventEntity
                    {
                        Type = serializedPayload.Type,
                        Payload = serializedPayload.Payload,
                        RowKey = e.EventId,
                        PartitionKey = GetEventPartitionKey(e.Source),
                        MessageId = e.MessageId,
                        StreamName = e.Source.StreamName,
                        AggregateId = e.Source.AggregateId,
                        EventId = e.EventId
                    };
                })
                .ToList();

            eventStore.Insert(eventEntities);

            eventEntities = eventEntities.Select(e => new EventEntity
            {
                Type = e.Type,
                Payload = e.Payload,
                RowKey = e.RowKey,
                PartitionKey = GetMessagePartitionKey(e.MessageId),
                MessageId = e.MessageId,
                StreamName = e.StreamName,
                AggregateId = e.AggregateId,
                EventId = e.EventId
            })
                .ToList();

            eventStore.Insert(eventEntities);
        }

        private static string GetMessagePartitionKey(string messageId)
        {
            return "in_" + messageId;
        }

        private static string GetEventPartitionKey(EventSource s)
        {
            return $"{s.StreamName}_{s.AggregateId}";
        }
    }

    public class EventSource
    {
        public EventSource(string streamName, string aggregateId)
        {
            StreamName = streamName;
            AggregateId = aggregateId;
        }

        public string StreamName { get; }
        public string AggregateId { get; }
    }

    public struct Event
    {
        public Event(string eventId, EventSource source, string messageId, object payload)
        {
            EventId = eventId;
            Source = source;
            MessageId = messageId;
            Payload = payload;
        }

        public string EventId { get; set; }
        public EventSource Source { get; }
        public string MessageId { get; set; }
        public object Payload { get; }
    }

    internal class EventEntity : TableEntity
    {
        public string MessageId { get; set; }
        public string Type { get; set; }
        public string Payload { get; set; }
        public string EventId { get; set; }
        public string AggregateId { get; set; }
        public string StreamName { get; set; }
        public DateTimeOffset? SynchronizedAt { get; set; }
    }
}