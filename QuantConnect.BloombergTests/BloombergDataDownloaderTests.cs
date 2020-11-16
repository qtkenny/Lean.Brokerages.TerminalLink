/*
* QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
* Lean Algorithmic Trading Engine v2.2 Copyright 2015 QuantConnect Corporation.
*/

using System;
using System.Linq;
using NUnit.Framework;
using QuantConnect.Bloomberg;
using QuantConnect.Bloomberg.Toolbox;
using QuantConnect.Configuration;
using QuantConnect.Logging;

namespace QuantConnect.BloombergTests
{
    [TestFixture, Ignore("These tests require a local Bloomberg terminal.")]
    public class BloombergDataDownloaderTests
    {
        [SetUp]
        public void SetUp()
        {
            Log.LogHandler = new ConsoleLogHandler();
            Config.SetConfigurationFile("../../integration-config.json");
            Config.Reset();
        }

        [TestCase("ES", Resolution.Minute)]
        public void DownloadsFuturesData(string ticker, Resolution resolution)
        {
            const SecurityType securityType = SecurityType.Future;

            using (var brokerage = BloombergCommon.CreateBrokerage())
            {
                var downloader = new BloombergDataDownloader(brokerage);

                var symbols = downloader.GetChainSymbols(ticker, securityType, true).ToList();

                var startDate = DateTime.UtcNow.Date.AddDays(-15);
                var endDate = DateTime.UtcNow.Date;

                downloader.DownloadAndSave(symbols, resolution, securityType, TickType.Trade, startDate, endDate);
                downloader.DownloadAndSave(symbols, resolution, securityType, TickType.Quote, startDate, endDate);
            }
        }
    }
}
