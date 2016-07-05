using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using Microsoft.WindowsAzure.Storage.Table.Queryable;

namespace AzureEventSourcing
{
    public class EntityId
    {
        public EntityId(string rowKey, string paritionKey)
        {
            RowKey = rowKey;
            ParitionKey = paritionKey;
        }

        public string RowKey { get; }
        public string ParitionKey { get; }
    }

    public class AzureTableStore<TEntity>
        where TEntity : TableEntity, new()
    {
        protected readonly CloudTable Table;

        public AzureTableStore(CloudStorageAccount account, string tableName)
        {
            if (account == null)
            {
                throw new ArgumentNullException(nameof(account));
            }

            Table = account
                .CreateCloudTableClient()
                .GetTableReference(tableName);

            Table.CreateIfNotExists();
        }

        public void Insert(TEntity entity)
        {
            Table.Execute(TableOperation.Insert(entity));
        }

        public void Insert(params TEntity[] entities)
        {
            Insert(entities.AsEnumerable());
        }

        public void Insert(IEnumerable<TEntity> entities)
        {
            foreach (var entity in entities)
            {
                Table.Execute(TableOperation.Insert(entity));
            }
        }

        public void Update(TEntity entity)
        {
            Table.Execute(TableOperation.Replace(entity));
        }

        public TEntity GetSingle(EntityId id)
        {
            var entity = Table.Execute(TableOperation.Retrieve<TEntity>(id.ParitionKey, id.RowKey))
                .Result as TEntity;

            return entity;
        }

        public IEnumerable<TEntity> GetPartition(string partitionKey)
        {
            var query = Table.CreateQuery<TEntity>()
                .Where(entity => entity.PartitionKey == partitionKey)
                .AsTableQuery();

            return Table.ExecuteQuery(query);
        }
    }
}