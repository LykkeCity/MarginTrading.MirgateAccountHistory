using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using AzureStorage;
using Lykke.AzureStorage.Tables.Paging;
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
    }

    public class MarginTradingAccountHistoryRepository
    {
        private readonly INoSQLTableStorage<MarginTradingAccountHistoryEntity> _tableStorage;

        public MarginTradingAccountHistoryRepository(INoSQLTableStorage<MarginTradingAccountHistoryEntity> tableStorage)
        {
            _tableStorage = tableStorage;
        }

        public async Task Read(TableQuery<MarginTradingAccountHistoryEntity> tableQuery, Func<IReadOnlyList<MarginTradingAccountHistoryEntity>, Task> onNewPage)
        {
            var pagingInfo = new PagingInfo();
            while (true)
            {
                var data = await _tableStorage.ExecuteQueryWithPaginationAsync(tableQuery, pagingInfo);
                pagingInfo = data.PagingInfo;

                if (data?.Result == null)
                {
                    break;
                }

                var result = data.Result.ToList();

                await onNewPage(result);

                if (data?.PagingInfo?.NextPage == null)
                {
                    break;
                }
            }
        }

        public Task BatchDelete(TableQuery<MarginTradingAccountHistoryEntity> tableQuery)
        {
            return Read(tableQuery,
                page => Task.WhenAll(page.GroupBy(p => p.PartitionKey).Select(_tableStorage.DeleteAsync)));
        }

        public Task Insert(IReadOnlyCollection<MarginTradingAccountHistoryEntity> src)
        {
            return _tableStorage.InsertOrReplaceBatchAsync(src);
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
                Id = s.RowKey,
                Type = s.Type,
                WithdrawTransferLimit = s.WithdrawTransferLimit,
            }).ToList();

            return _tableStorage.InsertBatchAndGenerateRowKeyAsync(entities,
                (e, retry, itemNum) =>
                    e.Date.ToString(RowKeyDateTimeFormat.Iso.ToDateTimeMask()) +
                    (retry * entities.Count + itemNum).ToDateTimeSuffix(RowKeyDateTimeFormat.Iso));
        }
    }
}
