﻿/*
* QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
* Lean Algorithmic Trading Engine v2.2 Copyright 2015 QuantConnect Corporation.
*/

using System;
using QuantConnect.Data;
using QuantConnect.Util;
using QuantConnect.Securities;
using System.Collections.Generic;
using QuantConnect.Configuration;

namespace QuantConnect.TerminalLink.Toolbox
{
    public class TerminalLinkDataDownloader : IDataDownloader
    {
        private readonly TerminalLinkBrokerage _brokerage;

        public TerminalLinkDataDownloader() : this(new TerminalLinkBrokerage(Config.GetValue<ApiType>("terminal-link-api-type"),
                Config.GetValue<Environment>("terminal-link-environment"),
                Config.Get("terminal-link-server-host"),
                Config.GetInt("terminal-link-server-port"),
                new TerminalLinkSymbolMapper(),
                Composer.Instance.GetExportedValueByTypeName<IDataAggregator>(Config.Get("data-aggregator", "QuantConnect.Lean.Engine.DataFeeds.AggregationManager"))))
        {
            
        }

        public TerminalLinkDataDownloader(TerminalLinkBrokerage brokerage)
        {
            _brokerage = brokerage;
        }

        /// <summary>
        /// Get historical data enumerable for a single symbol, type and resolution given this start and end time (in UTC).
        /// </summary>
        /// <param name="dataDownloaderGetParameters">model class for passing in parameters for historical data</param>
        /// <returns>Enumerable of base data for this symbol</returns>
        public IEnumerable<BaseData> Get(DataDownloaderGetParameters dataDownloaderGetParameters)
        {
            var symbol = dataDownloaderGetParameters.Symbol;
            var resolution = dataDownloaderGetParameters.Resolution;
            var startUtc = dataDownloaderGetParameters.StartUtc;
            var endUtc = dataDownloaderGetParameters.EndUtc;
            var tickType = dataDownloaderGetParameters.TickType;

            var exchangeHours = MarketHoursDatabase.FromDataFolder().GetExchangeHours(symbol.ID.Market, symbol, symbol.SecurityType);
            var dataTimeZone = MarketHoursDatabase.FromDataFolder().GetDataTimeZone(symbol.ID.Market, symbol, symbol.SecurityType);

            IEnumerable<Symbol> symbols = new List<Symbol> { symbol };
            if (symbol.IsCanonical())
            {
                symbols = _brokerage.LookupSymbols(symbol, true);
            }

            var dataType = LeanData.GetDataType(resolution, tickType);

            foreach (var targetSymbol in symbols)
            {
                foreach (var data in _brokerage.GetHistory(new HistoryRequest(startUtc, endUtc, dataType, targetSymbol, resolution, exchangeHours,
                    dataTimeZone, fillForwardResolution: null, true, false, DataNormalizationMode.Raw, tickType)))
                {
                    yield return data;
                }
            }
        }

        /// <summary>
        ///     Returns an IEnumerable of Future/Option contract symbols for the given root ticker
        /// </summary>
        /// <param name="ticker">The root ticker</param>
        /// <param name="securityType">Expected security type of the returned symbols (if any)</param>
        /// <param name="includeExpired">Include expired contracts</param>
        public IEnumerable<Symbol> GetChainSymbols(string ticker, SecurityType securityType, bool includeExpired)
        {
            var symbolMapper = new TerminalLinkSymbolMapper();
            var market = symbolMapper.GetMarket(ticker) ?? Market.USA;
            var canonicalSymbol = Symbol.Create(ticker, securityType, market);

            return _brokerage.LookupSymbols(canonicalSymbol, includeExpired);
        }

        /// <summary>
        ///     Downloads historical data from the brokerage and saves it in LEAN format.
        /// </summary>
        /// <param name="symbols">The list of symbols</param>
        /// <param name="tickType">The tick type</param>
        /// <param name="resolution">The resolution</param>
        /// <param name="securityType">The security type</param>
        /// <param name="startTimeUtc">The starting date/time (UTC)</param>
        /// <param name="endTimeUtc">The ending date/time (UTC)</param>
        public void DownloadAndSave(List<Symbol> symbols, Resolution resolution, SecurityType securityType, TickType tickType, DateTime startTimeUtc, DateTime endTimeUtc)
        {
            var writer = new LeanDataWriter(Globals.DataFolder, resolution, securityType, tickType);
            writer.DownloadAndSave(_brokerage, symbols, startTimeUtc, endTimeUtc);
        }
    }
}