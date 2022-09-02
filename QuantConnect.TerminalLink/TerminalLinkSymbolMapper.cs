/*
* QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
* Lean Algorithmic Trading Engine v2.2 Copyright 2015 QuantConnect Corporation.
*/

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using Newtonsoft.Json;
using QuantConnect.Configuration;
using QuantConnect.Logging;
using QuantConnect.Securities.Future;
using QuantConnect.Util;
using static QuantConnect.StringExtensions;

namespace QuantConnect.TerminalLink
{
    /// <summary>
    /// Provides the mapping between Lean symbols and TerminalLink symbols.
    /// </summary>
    public class TerminalLinkSymbolMapper : ITerminalLinkSymbolMapper
    {
        private readonly object _lock = new object();

        // Manual mapping of TerminalLink tickers to Lean symbols
        private readonly Dictionary<string, Symbol> _mapTerminalLinkToLean = new Dictionary<string, Symbol>();

        // Manual mapping of Lean symbols back to TerminalLink tickets
        private readonly Dictionary<Symbol, string> _mapLeanToTerminalLink = new Dictionary<Symbol, string>();

        private readonly HashSet<string> _forexCurrencies = Currencies.CurrencySymbols.Keys.ToHashSet();

        private readonly List<string> _futuresMonthCodes = new List<string> { "F", "G", "H", "J", "K", "M", "N", "Q", "U", "V", "X", "Z" };
        private readonly Regex _futureSymbolRegex = new Regex(@"^(?<RootTicker>[A-Z\s]+)[0-9]+\s", RegexOptions.Compiled);

        public TerminalLinkSymbolMapper() : this(Config.Get("terminal-link-symbol-map-file")) { }

        /// <summary>
        /// Constructs TerminalLinkSymbolMapper
        /// </summary>
        /// <param name="bbNameMapFullName">Full file name of the map file</param>
        public TerminalLinkSymbolMapper(string bbNameMapFullName)
        {
            if (string.IsNullOrEmpty(bbNameMapFullName))
            {
                bbNameMapFullName = "terminal-link-symbol-map.json";
            }
            if (!File.Exists(bbNameMapFullName))
            {
                // in the research environment binaries and their payload do not live in the current working directory
                // so let's check the composer-dll-directory
                var exception = new Exception($"Symbol map file not found: {bbNameMapFullName}");
                var composerDirectory = Config.Get("composer-dll-directory");
                if (string.IsNullOrEmpty(composerDirectory))
                {
                    throw exception;
                }

                bbNameMapFullName = Path.Combine(composerDirectory, bbNameMapFullName);
                if (!File.Exists(bbNameMapFullName))
                {
                    throw exception;
                }
            }

            try
            {
                MappingInfo = JsonConvert.DeserializeObject<Dictionary<string, TerminalLinkMappingInfo>>(File.ReadAllText(bbNameMapFullName));
            }
            catch (Exception e)
            {
                Log.Error(e, "Unable to load file: " + bbNameMapFullName);
            }
            finally
            {
                if (MappingInfo == null) MappingInfo = new Dictionary<string, TerminalLinkMappingInfo>(0);
            }
        }

        /// <summary>
        /// Converts a Lean symbol instance to a TerminalLink symbol
        /// </summary>
        /// <param name="symbol">A Lean symbol instance</param>
        /// <returns>The TerminalLink symbol</returns>
        public virtual string GetBrokerageSymbol(Symbol symbol)
        {
            if (symbol == null || string.IsNullOrWhiteSpace(symbol.Value))
                throw new ArgumentException("Invalid symbol: " + (symbol == null ? "null" : symbol.ToString()));

            if (symbol.IsCanonical())
            {
                switch (symbol.SecurityType)
                {
                    case SecurityType.Option:
                        return GetTerminalLinkTopicName(symbol.Underlying);

                    case SecurityType.Future:
                        return GetTerminalLinkTopicName(symbol);

                    default:
                        throw new ArgumentException($"Invalid security type for canonical symbol: {symbol.SecurityType}");
                }
            }

            return GetTerminalLinkTopicName(symbol);
        }

        /// <summary>
        /// Converts a TerminalLink symbol to a Lean symbol instance
        /// </summary>
        /// <param name="brokerageSymbol">The TerminalLink symbol</param>
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
        /// Converts a TerminalLink symbol to a Lean symbol instance
        /// </summary>
        /// <param name="brokerageSymbol">The TerminalLink symbol</param>
        /// <param name="securityType">The security type</param>
        public virtual Symbol GetLeanSymbol(string brokerageSymbol, SecurityType? securityType)
        {
            if (string.IsNullOrWhiteSpace(brokerageSymbol))
                throw new ArgumentException("Invalid brokerage symbol: " + brokerageSymbol);

            if (securityType == SecurityType.Future)
            {
                lock (_lock)
                {
                    if (!_mapTerminalLinkToLean.TryGetValue(brokerageSymbol, out var leanSymbol))
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
        /// Converts a TerminalLink symbol to a Lean symbol instance
        /// </summary>
        /// <param name="brokerageSymbol">The TerminalLink symbol</param>
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

        public Dictionary<string, TerminalLinkMappingInfo> MappingInfo { get; }

        private bool TryLookupMappingInfo(Symbol symbol, out KeyValuePair<string, TerminalLinkMappingInfo> mappingInfo)
        {
            return TryLookupMappingInfo(symbol.ID.Symbol, out mappingInfo);
        }

        private bool TryLookupMappingInfo(string underlying, out KeyValuePair<string, TerminalLinkMappingInfo> mappingInfo)
        {
            mappingInfo = MappingInfo.FirstOrDefault(x => x.Value.Underlying == underlying);
            return mappingInfo.Value != null;
        }

        private string GetTerminalLinkTopicName(Symbol symbol)
        {
            if (symbol.SecurityType == SecurityType.Future)
            {
                lock (_lock)
                {
                    if (!_mapLeanToTerminalLink.TryGetValue(symbol, out var ticker))
                    {
                        ticker = BuildFutureTickerFromLeanSymbol(symbol);
                    }
                    return ticker;
                }
            }

            var topicName = GetTerminalLinkSymbol(symbol);

            var terminalLinkMarket = GetTerminalLinkMarket(symbol.ID.Market, symbol.SecurityType);
            if (terminalLinkMarket.Length > 0)
            {
                topicName += $" {terminalLinkMarket}";
            }

            topicName += $" {GetTerminalLinkSecurityType(symbol.SecurityType)}";

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
                _mapTerminalLinkToLean[ticker] = symbol;
                _mapLeanToTerminalLink[symbol] = ticker;
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

        public string GetRootTicker(string brokerageSymbol)
        {
            var match = _futureSymbolRegex.Match(brokerageSymbol);
            var rootTicker = match.Groups["RootTicker"].Value;
            return !string.IsNullOrWhiteSpace(rootTicker) && _futuresMonthCodes.Contains(rootTicker.Substring(rootTicker.Length - 1))
                ? rootTicker.Substring(0, rootTicker.Length - 1)
                : rootTicker;
        }

        private bool TryBuildLeanSymbolFromFutureTicker(string brokerageSymbol, out Symbol symbol)
        {
            symbol = null;
            if (brokerageSymbol.Length < 2)
            {
                // Future ticker length must be at least 2.
                return false;
            }

            var rootTicker = GetRootTicker(brokerageSymbol);

            if (!MappingInfo.TryGetValue(rootTicker, out var info))
            {
                // Root ticker not found in futures map file
                return false;
            }

            var parts = brokerageSymbol.Substring(rootTicker.Length).Split(' '); 
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
                _mapTerminalLinkToLean[brokerageSymbol] = symbol;
                _mapLeanToTerminalLink[symbol] = brokerageSymbol;
            }

            return true;
        }

        private static Symbol CreateUnderlyingSymbol(TerminalLinkMappingInfo info)
        {
            return Symbol.Create(info.Underlying, SecurityType.Future, info.Market, $"/{info.Underlying}");
        }

        private string GetTerminalLinkSymbol(Symbol symbol)
        {
            if (symbol.SecurityType == SecurityType.Forex)
            {
                return symbol.Value;
            }

            if (symbol.SecurityType == SecurityType.Option)
            {
                // Equity options: Root Ticker x Exchange Code x Expiry MM/DD/YY (or Expiry M/Y only) x C or P x Strike Price
                return $"{symbol.Underlying.Value} UO {symbol.ID.Date:MM/dd/yy} {(symbol.ID.OptionRight == OptionRight.Call ? "C" : "P")}{symbol.ID.StrikePrice:F2}";
            }

            return symbol.Value;
        }

        private string GetTerminalLinkMarket(string market, SecurityType securityType)
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

        private string GetTerminalLinkSecurityType(SecurityType securityType)
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
