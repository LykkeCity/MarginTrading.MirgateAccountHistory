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

                Console.WriteLine("Converting..");
                await Task.WhenAll(programDemo.RunConvert(), programLive.RunConvert());
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
        private readonly MarginTradingAccountHistoryRepository _repository2;
        private readonly MarginTradingAccountHistoryRepository _repository3;

        private Program(string connection, string envName)
        {
            _envName = envName;
            var connectionString = connection.MakeSettings();
            _log = GetLog(connectionString, "MarginTradingMirgateAccountHistoryLogs");

            _repository = new MarginTradingAccountHistoryRepository(
                AzureTableStorage<MarginTradingAccountHistoryEntity>.Create(
                    connectionString,
                    "MarginTradingAccountsHistory", _log));
            _repository2 = new MarginTradingAccountHistoryRepository(
                AzureTableStorage<MarginTradingAccountHistoryEntity>.Create(
                    connectionString,
                    "AccountsHistory", new EmptyLog()));
            _repository3 = new MarginTradingAccountHistoryRepository(
                AzureTableStorage<MarginTradingAccountHistoryEntity>.Create(
                    connectionString,
                    "MarginTradingAccountsHistoryOld", _log));
        }

        private async Task RunConvert()
        {
            _clock.Restart();
            await ProcessOperation(batch => Task.WhenAll(_repository2.AddWithDateKeyBatchAsync(batch), _repository3.Insert(batch)));
            Log("Convert finished");
        }

        private async Task ProcessOperation(Func<IReadOnlyList<MarginTradingAccountHistoryEntity>, Task> operation)
        {
            var errorTcs = new TaskCompletionSource<object>();
            var dataflow = DataflowFluent
                .ReceiveDataOfType<IReadOnlyList<MarginTradingAccountHistoryEntity>>()
                .TransformMany(batch => batch.GroupBy(p => p.PartitionKey, (k, gr) => gr.ToList()))
                .ProcessAsync(operation)
                .WithMaxDegreeOfParallelism(10)
                .Action(batch =>
                {
                    var oldCounterValue = _counter;
                    var currCounterValue = _counter += batch.Count;
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

            await _repository.Read(
                new TableQuery<MarginTradingAccountHistoryEntity>().Select(MarginTradingAccountHistoryEntity.ColumnNames),
                entities => dataflow.SendAsync(entities));
            await dataflow.CompleteAsync();
            errorTcs.TrySetResult(null);
            await errorTcs.Task;
            Log($"Fin. Completed: {_counter}; elapsed: {_clock.Elapsed}, speed: {_counter / _clock.Elapsed.TotalMinutes:f2}/min");
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
