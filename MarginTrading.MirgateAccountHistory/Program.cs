using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using AzureStorage.Tables;
using Common.Log;
using Lykke.Logs;
using Lykke.SettingsReader;
using MarginTrading.MirgateAccountHistory.Helpers;
using Microsoft.Extensions.Configuration;
using Microsoft.WindowsAzure.Storage.Table;
using Rocks.Dataflow.Fluent;

namespace MarginTrading.MirgateAccountHistory
{
    class Program
    {
        private static readonly LogToConsole LogToConsole = new LogToConsole();

        static async Task Main()
        {
            Console.WriteLine("Start");
            try
            {
                var config = await GetConfigurationRoot();
                var programDemo = new Program(config.DemoHistoryDbConnectionString, "DEMO");
                var programLive = new Program(config.LiveHistoryDbConnectionString, "LIVE");

                Console.WriteLine("Press any key to start");
                Console.ReadKey();

                Console.WriteLine("If you wish to make a cleanup of previous unseccessful run - press 'c'. Otherwise - any other letter.");
                if (Console.ReadKey().KeyChar == 'c')
                {
                    Console.WriteLine();
                    Console.WriteLine("Cleaning up...");
                    await Task.WhenAll(programDemo.RunCleanup(), programLive.RunCleanup());
                }

                Console.WriteLine();
                Console.WriteLine("Converting..");
                await Task.WhenAll(programDemo.RunConvert(), programLive.RunConvert());

                while (Console.ReadKey().KeyChar != 'd')
                    Console.WriteLine("Generate new entities finished, press 'd' to delete old ones");

                Console.WriteLine();
                Console.WriteLine("Removing old..");
                await Task.WhenAll(programDemo.RunRemoveOldEntities(), programLive.RunRemoveOldEntities());
            }
            catch (Exception e)
            {
                Console.ForegroundColor = ConsoleColor.DarkYellow;
                await LogToConsole.WriteFatalErrorAsync(null, nameof(MirgateAccountHistory), "TOP LEVEL FAIL", e);
            }

            while (Console.ReadKey().KeyChar != 'q')
                Console.WriteLine("End. Press 'q' to exit.");
        }

        private readonly ILog _log;
        private int _counter;
        private readonly Stopwatch _clock = Stopwatch.StartNew();
        private readonly string _envName;
        private readonly MarginTradingAccountHistoryRepository _repository;
        private readonly MarginTradingAccountHistoryRepository _backup;

        private Program(string connection, string envName)
        {
            _envName = envName;
            var connectionString = connection.MakeSettings();
            _log = GetLog(connectionString, "MarginTradingMirgateAccountHistoryLogs");

            _repository = new MarginTradingAccountHistoryRepository(
                AzureTableStorage<MarginTradingAccountHistoryEntity>.Create(
                    connectionString,
                    "MarginTradingAccountsHistory", new EmptyLog()));
            _backup = new MarginTradingAccountHistoryRepository(AzureTableStorage<MarginTradingAccountHistoryEntity>.Create(
                connectionString,
                "MarginTradingAccountsHistoryBkp", _log));
        }

        private async Task RunCleanup()
        {
            _clock.Restart();
            await _repository.BatchDelete(new TableQuery<MarginTradingAccountHistoryEntity>()
                .Where(TableQuery.GenerateFilterConditionForInt("EntityVersion", QueryComparisons.Equal, 2)));
            Log("Cleanup unnecessary data finished");
        }

        private async Task RunConvert()
        {
            _clock.Restart();
            await ProcessOperation(e => e.EntityVersion != 2,
                batch => Task.WhenAll(_repository.AddWithDateKeyBatchAsync(batch),
                    _backup.InsertOrReplaceBatchAsync(batch)));
            Log("Convert finished");
        }

        private async Task RunRemoveOldEntities()
        {
            _clock.Restart();
            await ProcessOperation(e => e.EntityVersion != 2,
                batch => _repository.DeleteAsync(batch));
            Log("RemoveOldEntities finished");
        }

        private async Task ProcessOperation(
            Func<MarginTradingAccountHistoryEntity, bool> filter,
            Func<IReadOnlyList<MarginTradingAccountHistoryEntity>, Task> operation)
        {
            var errorTcs = new TaskCompletionSource<object>();
            var dataflow = DataflowFluent
                .ReceiveDataOfType<IReadOnlyList<MarginTradingAccountHistoryEntity>>()
                .TransformMany(batch =>
                    batch.Where(filter).GroupBy(p => p.PartitionKey, (k, gr) => gr.ToList()))
                .ProcessAsync(operation)
                .WithMaxDegreeOfParallelism(10)
                .Action(batch =>
                {
                    int currCounterValue;
                    int oldCounterValue;
                    //lock (_counterLock)
                    {
                        oldCounterValue = _counter;
                        currCounterValue = _counter += batch.Count;
                    }
                    if (currCounterValue / 1000 != oldCounterValue / 1000)
                    {
                        Log($"Completed: {currCounterValue}; elapsed: {_clock.Elapsed}, speed: {currCounterValue / _clock.Elapsed.TotalMinutes:f2}/min");
                    }
                })
                .WithMaxDegreeOfParallelism(1)
                .WithDefaultExceptionLogger((ex, obj) =>
                {
                    Error(ex);
                    errorTcs.TrySetException(ex);
                    throw ex;
                })
                .CreateDataflow();

            dataflow.Start();

            await _repository.ExecuteWithPaginationAsync(
                new TableQuery<MarginTradingAccountHistoryEntity>().Select(MarginTradingAccountHistoryEntity.ColumnNames),
                entities => dataflow.SendAsync(entities));
            await dataflow.CompleteAsync();
            errorTcs.TrySetResult(null);
            await errorTcs.Task;
        }

        private void Log(string str)
        {
            _log.WriteInfoAsync(null, nameof(MirgateAccountHistory), _envName, str);
        }
        private void Error(Exception ex)
        {
            _log.WriteFatalErrorAsync(null, nameof(MirgateAccountHistory), _envName, ex);
        }

        private static async Task<AppSettings> GetConfigurationRoot()
        {
            var builder =
                new ConfigurationBuilder().SetBasePath(Directory.GetCurrentDirectory())
                    .AddJsonFile("appsettings.dev.json", true, true)
                    .AddEnvironmentVariables();

            var configuration = builder.Build();

            var settingsUrl = configuration["SettingsUrl"];
            if (!string.IsNullOrWhiteSpace(settingsUrl))
            {
                var settingsFromUrl = await new HttpClient().GetStringAsync(settingsUrl);
                if (string.IsNullOrWhiteSpace(settingsFromUrl))
                {
                    throw new Exception("Could not download config file from url: " + settingsUrl);
                }

                configuration = builder
                    .Add(new JsonStringConfigurationSource(settingsFromUrl))
                    .Build();
            }
            return new AppSettings(configuration);
        }

        private static ILog GetLog(IReloadingManager<string> connectionString, string tableName)
        {
            var persistenceManager = new LykkeLogToAzureStoragePersistenceManager(
                AzureTableStorage<LogEntity>.Create(connectionString, tableName, LogToConsole),
                LogToConsole);

            var log = new LykkeLogToAzureStorage(persistenceManager, null, LogToConsole);
            log.Start();

            return new AggregateLogger(LogToConsole, log);
        }
    }
}
