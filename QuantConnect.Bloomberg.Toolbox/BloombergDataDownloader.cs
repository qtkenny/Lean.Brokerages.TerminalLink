/*
* QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
* Lean Algorithmic Trading Engine v2.2 Copyright 2015 QuantConnect Corporation.
*/

using System;
using System.Collections.Generic;
using QuantConnect.Data;
using QuantConnect.ToolBox;

namespace QuantConnect.Bloomberg.Toolbox
{
    public class BloombergDataDownloader : IDataDownloader
    {
        private readonly BloombergBrokerage _brokerage;

        public BloombergDataDownloader(BloombergBrokerage brokerage)
        {
            _brokerage = brokerage;
        }

        /// <summary>
        ///     Get historical data enumerable for a single symbol, type and resolution given this start and end time (in UTC).
        /// </summary>
        /// <param name="symbol">Symbol for the data we're looking for.</param>
        /// <param name="resolution">Resolution of the data request</param>
        /// <param name="startUtc">Start time of the data in UTC</param>
        /// <param name="endUtc">End time of the data in UTC</param>
        /// <returns>Enumerable of base data for this symbol</returns>
        public IEnumerable<BaseData> Get(Symbol symbol, Resolution resolution, DateTime startUtc, DateTime endUtc)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        ///     Returns an IEnumerable of Future/Option contract symbols for the given root ticker
        /// </summary>
        /// <param name="ticker">The root ticker</param>
        /// <param name="securityType">Expected security type of the returned symbols (if any)</param>
        /// <param name="includeExpired">Include expired contracts</param>
        public IEnumerable<Symbol> GetChainSymbols(string ticker, SecurityType securityType, bool includeExpired)
        {
            var symbolMapper = new BloombergSymbolMapper();
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