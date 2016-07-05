using System;
using System.Collections.Generic;
using AzureEventSourcing;
using Microsoft.WindowsAzure.Storage;
using Newtonsoft.Json;
using NodaTime;

namespace App
{
    class Program
    {
        class PayloadSerializer : IPayloadSerializer
        {
            public SerializedPayload Serialize(object payload)
            {
                var serialized = JsonConvert.SerializeObject(payload);

                return new SerializedPayload(serialized, payload.GetType().FullName);
            }

            public object Deserialize(SerializedPayload payload)
            {
                return JsonConvert.DeserializeObject(payload.Payload, Type.GetType(payload.Type));
            }
        }

        static void Main(string[] args)
        {
            var timezone = DateTimeZoneProviders.Tzdb["Europe/Riga"];

            var local = SystemClock.Instance.Now
                .InZone(timezone)
                .Date.PlusDays(1)
                .AtMidnight();

            var utc = timezone
                .AtLeniently(local)
                .ToInstant()
                .ToDateTimeOffset();

            //TestEventStore();

            //TestMessageStore();

            Console.ReadKey();
        }

        private static void TestMessageStore()
        {
            var messageStore = new MessageStore(CloudStorageAccount.DevelopmentStorageAccount, "testMessageStore");

            if (messageStore.Register("message-1", Tuple.Create("policyIssued")))
            {
                Console.WriteLine("Message registered");
            }
        }

        private static void TestEventStore()
        {
            var eventStore = new EventStore(CloudStorageAccount.DevelopmentStorageAccount, "testEventSourcing");

            var eventSource = new EventSource("events", "aggregate-1");
            const string messageId = "message-1";
            const string eventId = "00001";

            eventStore.Commit(new List<Event>
            {
                new Event(eventId, eventSource, messageId, Tuple.Create("policyIssued"))
            });

            var events = eventStore.GetStream(eventSource);

            foreach (var e in events)
            {
                Console.WriteLine(e.EventId);
            }

            Console.WriteLine("------------------------------");

            events = eventStore.GetStream(messageId);

            foreach (var e in events)
            {
                Console.WriteLine(e.EventId);
            }

            Console.WriteLine("------------------------------");

            eventStore.SetSynchronized(eventId, eventSource, DateTimeOffset.Now);
        }
    }
}
