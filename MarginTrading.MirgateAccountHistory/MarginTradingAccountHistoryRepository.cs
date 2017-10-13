using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using AzureStorage;
using Lykke.AzureStorage.Tables.Paging;
using MarginTrading.MirgateAccountHistory.Helpers;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace MarginTrading.MirgateAccountHistory
{
    public class MarginTradingAccountHistoryEntity : TableEntity
    {
        public static ImmutableList<string> ColumnNames = typeof(MarginTradingAccountHistoryEntity)
            .GetProperties(BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly)
            .Select(p => p.Name).ToImmutableList();

        public string Id { get; set; } // old row key
        public DateTime Date { get; set; }// existed as a field
        public string ClientId { get; set; }
        public double Amount { get; set; }
        public double Balance { get; set; }
        public double WithdrawTransferLimit { get; set; }
        public string Comment { get; set; }
        public string Type { get; set; }
        public int? EntityVersion { get; set; }

        public override void ReadEntity(IDictionary<string, EntityProperty> properties, OperationContext operationContext)
        {
            base.ReadEntity(properties, operationContext);
            switch (EntityVersion)
            {
                case 1:
                case null:
                    Id = RowKey;
                    return;
                case 2:
                    return;
                default:
                    throw new NotSupportedException($"Entity version {EntityVersion} is not suported. Only versions 1 & 2 are supported.");
            }
        }
    }

    public class MarginTradingAccountHistoryRepository
    {
        private readonly INoSQLTableStorage<MarginTradingAccountHistoryEntity> _tableStorage;

        public MarginTradingAccountHistoryRepository(INoSQLTableStorage<MarginTradingAccountHistoryEntity> tableStorage)
        {
            _tableStorage = tableStorage;
        }

        public async Task ExecuteWithPaginationAsync(TableQuery<MarginTradingAccountHistoryEntity> tableQuery, Func<IReadOnlyList<MarginTradingAccountHistoryEntity>, Task> onNewPage)
        {
            var pagingInfo = new AzurePagingInfo();
            while (true)
            {
                var data = await _tableStorage.ExecuteQueryWithPaginationAsync(tableQuery, pagingInfo);
                pagingInfo = (AzurePagingInfo)data.PagingInfo;
                if (data.ResultList.Count == 0)
                {
                    break;
                }

                await onNewPage(data.ResultList);
            }
        }

        public Task BatchDelete(TableQuery<MarginTradingAccountHistoryEntity> tableQuery)
        {
            return ExecuteWithPaginationAsync(tableQuery,
                page => Task.WhenAll(page.GroupBy(p => p.PartitionKey).Select(_tableStorage.DeleteAsync)));
        }

        public Task AddWithDateKeyBatchAsync(IReadOnlyCollection<MarginTradingAccountHistoryEntity> src)
        {
            var entities = src.Select(s => new MarginTradingAccountHistoryEntity
            {
                PartitionKey = s.PartitionKey,
                Amount = s.Amount,
                Balance = s.Balance,
                ClientId = s.ClientId,
                Comment = s.Comment,
                Date = s.Date,
                EntityVersion = 2,
                Id = s.Id,
                Type = s.Type,
                WithdrawTransferLimit = s.WithdrawTransferLimit,
            }).ToList();

            return _tableStorage.InsertBatchAndGenerateRowKeyAsync(entities,
                (e, retry, itemNum) =>
                    e.Date.ToString(RowKeyDateTimeFormat.Iso.ToDateTimeMask()) +
                    (retry * entities.Count + itemNum).ToDateTimeSuffix(RowKeyDateTimeFormat.Iso));
        }

        public Task InsertOrReplaceBatchAsync(IEnumerable<MarginTradingAccountHistoryEntity> batch)
        {
            return _tableStorage.InsertOrReplaceBatchAsync(batch);
        }

        public Task DeleteAsync(IReadOnlyList<MarginTradingAccountHistoryEntity> entities)
        {
            return _tableStorage.DeleteAsync(entities);
        }
    }
}
