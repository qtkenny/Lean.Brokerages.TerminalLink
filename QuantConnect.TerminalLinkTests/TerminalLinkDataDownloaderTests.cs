/*
* QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
* Lean Algorithmic Trading Engine v2.2 Copyright 2015 QuantConnect Corporation.
*/

using System;
using System.Linq;
using NUnit.Framework;
using QuantConnect.TerminalLink.Toolbox;
using QuantConnect.Configuration;
using QuantConnect.Logging;

namespace QuantConnect.TerminalLinkTests
{
    [TestFixture, Ignore("These tests require a local Bloomberg terminal.")]
    public class TerminalLinkDataDownloaderTests
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

            using (var brokerage = TerminalLinkCommon.CreateBrokerage())
            {
                var downloader = new TerminalLinkDataDownloader(brokerage);

                var symbols = downloader.GetChainSymbols(ticker, securityType, true).ToList();

                var startDate = DateTime.UtcNow.Date.AddDays(-15);
                var endDate = DateTime.UtcNow.Date;

                downloader.DownloadAndSave(symbols, resolution, securityType, TickType.Trade, startDate, endDate);
                downloader.DownloadAndSave(symbols, resolution, securityType, TickType.Quote, startDate, endDate);
            }
        }
    }
}
