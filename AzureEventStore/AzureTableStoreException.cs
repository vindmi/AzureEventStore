using System;
using System.Runtime.Serialization;

namespace AzureEventSourcing
{
    [Serializable]
    public class AzureTableStoreException : Exception
    {
        public AzureTableStoreException()
        {
        }

        public AzureTableStoreException(string message) : base(message)
        {
        }

        public AzureTableStoreException(string message, Exception inner) : base(message, inner)
        {
        }

        protected AzureTableStoreException(
            SerializationInfo info,
            StreamingContext context) : base(info, context)
        {
        }
    }
}
