/*
* QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
* Lean Algorithmic Trading Engine v2.2 Copyright 2015 QuantConnect Corporation.
*/

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using Newtonsoft.Json;
using QuantConnect.Configuration;
using QuantConnect.Logging;
using QuantConnect.Securities.Future;
using QuantConnect.Util;
using static QuantConnect.StringExtensions;

namespace QuantConnect.Bloomberg
{
    /// <summary>
    /// Provides the mapping between Lean symbols and Bloomberg symbols.
    /// </summary>
    public class BloombergSymbolMapper : IBloombergSymbolMapper
    {
        private readonly object _lock = new object();

        // Manual mapping of Bloomberg tickers to Lean symbols
        private readonly Dictionary<string, Symbol> _mapBloombergToLean = new Dictionary<string, Symbol>();

        // Manual mapping of Lean symbols back to Bloomberg tickets
        private readonly Dictionary<Symbol, string> _mapLeanToBloomberg = new Dictionary<Symbol, string>();

        private readonly HashSet<string> _forexCurrencies = Currencies.CurrencyPairs.ToHashSet();

        public BloombergSymbolMapper() : this(Config.Get("bloomberg-symbol-map-file", "bloomberg-symbol-map.json")) { }

        /// <summary>
        /// Constructs BloombergSymbolMapper
        /// </summary>
        /// <param name="bbNameMapFullName">Full file name of the map file</param>
        public BloombergSymbolMapper(string bbNameMapFullName)
        {
            if (!File.Exists(bbNameMapFullName))
            {
                throw new Exception($"Symbol map file not found: {bbNameMapFullName}");
            }

            try
            {
                MappingInfo = JsonConvert.DeserializeObject<Dictionary<string, BloombergMappingInfo>>(File.ReadAllText(bbNameMapFullName));
            }
            catch (Exception e)
            {
                Log.Error(e, "Unable to load file: " + bbNameMapFullName);
            }
            finally
            {
                if (MappingInfo == null) MappingInfo = new Dictionary<string, BloombergMappingInfo>(0);
            }
        }

        /// <summary>
        /// Converts a Lean symbol instance to a Bloomberg symbol
        /// </summary>
        /// <param name="symbol">A Lean symbol instance</param>
        /// <returns>The Bloomberg symbol</returns>
        public virtual string GetBrokerageSymbol(Symbol symbol)
        {
            if (symbol == null || string.IsNullOrWhiteSpace(symbol.Value))
                throw new ArgumentException("Invalid symbol: " + (symbol == null ? "null" : symbol.ToString()));

            if (symbol.IsCanonical())
            {
                switch (symbol.SecurityType)
                {
                    case SecurityType.Option:
                        return GetBloombergTopicName(symbol.Underlying);

                    case SecurityType.Future:
                        return GetBloombergTopicName(symbol);

                    default:
                        throw new ArgumentException($"Invalid security type for canonical symbol: {symbol.SecurityType}");
                }
            }

            return GetBloombergTopicName(symbol);
        }

        /// <summary>
        /// Converts a Bloomberg symbol to a Lean symbol instance
        /// </summary>
        /// <param name="brokerageSymbol">The Bloomberg symbol</param>
        /// <param name="securityType">The security type</param>
        /// <param name="market">The market</param>
        /// <param name="expirationDate">Expiration date of the security (if applicable)</param>
        /// <param name="strike">The strike of the security (if applicable)</param>
        /// <param name="optionRight">The option right of the security (if applicable)</param>
        /// <returns>A new Lean Symbol instance</returns>
        public virtual Symbol GetLeanSymbol(
            string brokerageSymbol,
            SecurityType securityType,
            string market,
            DateTime expirationDate = new DateTime(),
            decimal strike = 0,
            OptionRight optionRight = OptionRight.Call)
        {
            return GetLeanSymbol(brokerageSymbol, securityType);
        }

        /// <summary>
        /// Converts a Bloomberg symbol to a Lean symbol instance
        /// </summary>
        /// <param name="brokerageSymbol">The Bloomberg symbol</param>
        /// <param name="securityType">The security type</param>
        public virtual Symbol GetLeanSymbol(string brokerageSymbol, SecurityType? securityType)
        {
            if (string.IsNullOrWhiteSpace(brokerageSymbol))
                throw new ArgumentException("Invalid brokerage symbol: " + brokerageSymbol);

            if (securityType == SecurityType.Future)
            {
                lock (_lock)
                {
                    if (!_mapBloombergToLean.TryGetValue(brokerageSymbol, out var leanSymbol))
                    {
                        if (TryBuildLeanSymbolFromFutureTicker(brokerageSymbol, out leanSymbol))
                        {
                            return leanSymbol;
                        }

                        throw new Exception($"Ticker cannot be parsed as a future symbol: {brokerageSymbol}");
                    }

                    return leanSymbol;
                }
            }

            return GetLeanSymbol(brokerageSymbol);
        }

        public string[] GetManualChain(Symbol symbol)
        {
            return TryLookupMappingInfo(symbol, out var mappingInfo) && mappingInfo.Value.Chain?.Length > 0 ? mappingInfo.Value.Chain : null;
        }

        public string GetMarket(string canonicalSymbol)
        {
            return TryLookupMappingInfo(canonicalSymbol, out var mappingInfo) && !string.IsNullOrWhiteSpace(mappingInfo.Value.Market) ? mappingInfo.Value.Market : null;
        }

        /// <summary>
        /// Converts a Bloomberg symbol to a Lean symbol instance
        /// </summary>
        /// <param name="brokerageSymbol">The Bloomberg symbol</param>
        /// <returns>A new Lean Symbol instance</returns>
        private Symbol GetLeanSymbol(string brokerageSymbol)
        {
            if (TryBuildLeanSymbolFromFutureTicker(brokerageSymbol, out var leanSymbol))
            {
                return leanSymbol;
            }

            if (TryBuildLeanSymbolFromForexTicker(brokerageSymbol, out leanSymbol))
            {
                return leanSymbol;
            }

            if (TryBuildLeanSymbolFromOptionTicker(brokerageSymbol, out leanSymbol))
            {
                return leanSymbol;
            }

            if (TryBuildLeanSymbolFromEquityTicker(brokerageSymbol, out leanSymbol))
            {
                return leanSymbol;
            }

            throw new ArgumentException("Invalid brokerage symbol: " + brokerageSymbol);
        }

        public Dictionary<string, BloombergMappingInfo> MappingInfo { get; }

        private bool TryLookupMappingInfo(Symbol symbol, out KeyValuePair<string, BloombergMappingInfo> mappingInfo)
        {
            return TryLookupMappingInfo(symbol.ID.Symbol, out mappingInfo);
        }

        private bool TryLookupMappingInfo(string underlying, out KeyValuePair<string, BloombergMappingInfo> mappingInfo)
        {
            mappingInfo = MappingInfo.FirstOrDefault(x => x.Value.Underlying == underlying);
            return mappingInfo.Value != null;
        }

        private string GetBloombergTopicName(Symbol symbol)
        {
            if (symbol.SecurityType == SecurityType.Future)
            {
                lock (_lock)
                {
                    if (!_mapLeanToBloomberg.TryGetValue(symbol, out var ticker))
                    {
                        ticker = BuildFutureTickerFromLeanSymbol(symbol);
                    }
                    return ticker;
                }
            }

            var topicName = GetBloombergSymbol(symbol);

            var bloombergMarket = GetBloombergMarket(symbol.ID.Market, symbol.SecurityType);
            if (bloombergMarket.Length > 0)
            {
                topicName += $" {bloombergMarket}";
            }

            topicName += $" {GetBloombergSecurityType(symbol.SecurityType)}";

            return topicName;
        }

        private string BuildFutureTickerFromLeanSymbol(Symbol symbol)
        {
            if (!TryLookupMappingInfo(symbol, out var entry))
            {
                throw new Exception($"Lean ticker not found: {symbol.Value}");
            }

            string ticker;
            if (symbol.IsCanonical())
            {
                ticker = $"{entry.Key}{entry.Value.RootLookupSuffix} {entry.Value.TickerSuffix}";
            }
            else
            {
                var brokerageTicker = symbol.Value.Substring(symbol.ID.Symbol.Length).Substring(2);
                brokerageTicker = $"{brokerageTicker[0]}{brokerageTicker[2]}";
                ticker = $"{entry.Key}{brokerageTicker} {entry.Value.TickerSuffix}";
            }

            lock (_lock)
            {
                _mapBloombergToLean[ticker] = symbol;
                _mapLeanToBloomberg[symbol] = ticker;
            }

            return ticker;
        }

        private bool TryBuildLeanSymbolFromForexTicker(string brokerageSymbol, out Symbol symbol)
        {
            symbol = null;

            var parts = brokerageSymbol.Split(' ');
            if (parts.Length != 2)
            {
                return false;
            }

            if (parts[1] != "Curncy")
            {
                return false;
            }

            var ticker = parts[0];

            if (ticker.Length != 6)
            {
                return false;
            }

            if (!_forexCurrencies.Contains(ticker))
            {
                return false;
            }

            symbol = Symbol.Create(ticker, SecurityType.Forex, Market.FXCM);

            return true;
        }

        private bool TryBuildLeanSymbolFromOptionTicker(string brokerageSymbol, out Symbol symbol)
        {
            symbol = null;

            var parts = brokerageSymbol.Split(' ');

            if (parts.Length != 5)
            {
                return false;
            }

            var underlying = parts[0];

            if (parts[3].Length < 2)
            {
                return false;
            }

            var callOrPut = parts[3][0];
            if (callOrPut != 'C' && callOrPut != 'P')
            {
                return false;
            }

            var right = callOrPut == 'C' ? OptionRight.Call : OptionRight.Put;

            var strikeString = parts[3].Substring(1);

            if (!decimal.TryParse(strikeString, NumberStyles.Any, CultureInfo.InvariantCulture, out var strike))
            {
                return false;
            }

            if (!DateTime.TryParseExact(parts[2], "MM/dd/yy", CultureInfo.InvariantCulture, DateTimeStyles.None, out var expiry))
            {
                return false;
            }

            symbol = Symbol.CreateOption(underlying, Market.USA, OptionStyle.American, right, strike, expiry);

            return true;
        }

        private bool TryBuildLeanSymbolFromEquityTicker(string brokerageSymbol, out Symbol symbol)
        {
            symbol = null;

            var parts = brokerageSymbol.Split(' ');

            if (parts.Length != 3)
            {
                return false;
            }

            if (parts[2] != "Equity")
            {
                return false;
            }

            symbol = Symbol.Create(parts[0], SecurityType.Equity, Market.USA);

            return true;
        }

        private bool TryBuildLeanSymbolFromFutureTicker(string brokerageSymbol, out Symbol symbol)
        {
            symbol = null;
            if (brokerageSymbol.Length < 2)
            {
                // Future ticker length must be at least 2.
                return false;
            }

            var rootTicker = brokerageSymbol.Substring(0, 2);

            if (!MappingInfo.TryGetValue(rootTicker, out var info))
            {
                // Root ticker not found in futures map file
                return false;
            }

            var parts = brokerageSymbol.Substring(2).Split(' '); 
            var underlyingSymbol = CreateUnderlyingSymbol(info);

            if (parts[0] == info.RootLookupSuffix)
            {
                // canonical future symbol
                symbol = underlyingSymbol;
            }
            else
            {
                // future contract
                var ticker = info.Underlying + parts[0];

                var newTicker = ticker;
                var properties = SymbolRepresentation.ParseFutureTicker(newTicker);
                var expiryFunc = FuturesExpiryFunctions.FuturesExpiryFunction(underlyingSymbol);

                var year = DateTime.UtcNow.Year;
                year = year - year % (properties.ExpirationYearShort < 10 ? 10 : 100) + properties.ExpirationYearShort;

                var expiryDate = expiryFunc(new DateTime(year, properties.ExpirationMonth, properties.ExpirationDay));
                symbol = Symbol.CreateFuture(info.Underlying, info.Market, expiryDate);
            }

            lock (_lock)
            {
                _mapBloombergToLean[brokerageSymbol] = symbol;
                _mapLeanToBloomberg[symbol] = brokerageSymbol;
            }

            return true;
        }

        private static Symbol CreateUnderlyingSymbol(BloombergMappingInfo info)
        {
            return Symbol.Create(info.Underlying, SecurityType.Future, info.Market, $"/{info.Underlying}");
        }

        private string GetBloombergSymbol(Symbol symbol)
        {
            if (symbol.SecurityType == SecurityType.Forex)
            {
                return symbol.Value;
            }

            if (symbol.SecurityType == SecurityType.Option)
            {
                // Equity options: Root Ticker x Exchange Code x Expiry MM/DD/YY (or Expiry M/Y only) x C or P x Strike Price
                return Invariant($"{symbol.Underlying.Value} UO {symbol.ID.Date:MM/dd/yy} {(symbol.ID.OptionRight == OptionRight.Call ? "C" : "P")}{symbol.ID.StrikePrice:F2}");
            }

            return symbol.Value;
        }

        private string GetBloombergMarket(string market, SecurityType securityType)
        {
            if (securityType == SecurityType.Forex)
            {
                return string.Empty;
            }

            if (securityType == SecurityType.Future)
            {
                return "COMB";
            }

            if (securityType == SecurityType.Option)
            {
                // exchange/market is already included in the ticker
                return "";
            }

            switch (market)
            {
                case Market.USA:
                    return "US";

                default:
                    throw new NotSupportedException($"Unsupported market: {market} for security type: {securityType}");
            }
        }

        private string GetBloombergSecurityType(SecurityType securityType)
        {
            switch (securityType)
            {
                case SecurityType.Equity:
                    return "Equity";

                case SecurityType.Forex:
                    return "Curncy";

                case SecurityType.Option:
                    // only equity options for now
                    return "Equity";

                case SecurityType.Future:
                    // for Futures, mapping is done in the caller using the map file and this method is not called
                default:
                    throw new NotSupportedException($"Unsupported security type: {securityType}");
            }
        }
    }
}
