using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.Extensions.Configuration;

namespace MarginTrading.MirgateAccountHistory
{
    public class AppSettings
    {
        private readonly IConfigurationRoot _configurationRoot;
        public AppSettings(IConfigurationRoot configurationRoot)
        {
            _configurationRoot = configurationRoot;
        }

        public string LiveHistoryDbConnectionString => _configurationRoot.GetValue<string>("MtBackend:MarginTradingLive:Db:HistoryConnString");
        public string DemoHistoryDbConnectionString => _configurationRoot.GetValue<string>("MtBackend:MarginTradingDemo:Db:HistoryConnString");
        public string LogsConnString => "MtBackend:MarginTradingDemo:Db:LogsConnString";
    }
}
