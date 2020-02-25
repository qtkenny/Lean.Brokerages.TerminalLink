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
using QuantConnect.Logging;
using QuantConnect.Securities.Future;
using static QuantConnect.StringExtensions;

namespace QuantConnect.Bloomberg
{
    /// <summary>
    /// Provides the mapping between Lean symbols and Bloomberg symbols.
    /// </summary>
    public class BloombergSymbolMapper : IBloombergSymbolMapper
    {
        // Manual mapping of Bloomberg tickers to Lean symbols
        private readonly Dictionary<string, Symbol> _mapBloombergToLean = new Dictionary<string, Symbol>();

        // Manual mapping of Lean symbols back to Bloomberg tickets
        private readonly Dictionary<Symbol, string> _mapLeanToBloomberg = new Dictionary<Symbol, string>();

        public BloombergSymbolMapper() : this("bloomberg-symbol-map.json") { }

        /// <summary>
        /// Constructs BloombergSymbolMapper
        /// </summary>
        /// <param name="bbNameMapFullName">Full file name of the map file</param>
        public BloombergSymbolMapper(string bbNameMapFullName)
        {
            if (!File.Exists(bbNameMapFullName)) return;

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

            _mapBloombergToLean = new Dictionary<string, Symbol>();
            _mapLeanToBloomberg = new Dictionary<Symbol, string>();
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
            return GetLeanSymbol(brokerageSymbol);
        }

        /// <summary>
        /// Converts a Bloomberg symbol to a Lean symbol instance
        /// </summary>
        /// <param name="brokerageSymbol">The Bloomberg symbol</param>
        /// <param name="securityType">The security type</param>
        public virtual Symbol GetLeanSymbol(string brokerageSymbol, SecurityType securityType)
        {
            if (string.IsNullOrWhiteSpace(brokerageSymbol))
                throw new ArgumentException("Invalid brokerage symbol: " + brokerageSymbol);

            if (securityType == SecurityType.Future)
            {
                if (!_mapBloombergToLean.TryGetValue(brokerageSymbol, out var leanSymbol))
                {
                    leanSymbol = BuildLeanSymbolFromFutureTicker(brokerageSymbol);
                }
                return leanSymbol;
            }

            return GetLeanSymbol(brokerageSymbol);
        }

        /// <summary>
        /// Converts a Bloomberg symbol to a Lean symbol instance
        /// </summary>
        /// <param name="brokerageSymbol">The Bloomberg symbol</param>
        /// <returns>A new Lean Symbol instance</returns>
        public virtual Symbol GetLeanSymbol(string brokerageSymbol)
        {
            if (string.IsNullOrWhiteSpace(brokerageSymbol))
                throw new ArgumentException("Invalid brokerage symbol: " + brokerageSymbol);

            if (_mapBloombergToLean.TryGetValue(brokerageSymbol, out var leanSymbol))
            {
                return leanSymbol;
            }

            var parts = brokerageSymbol.Split(' ');

            var securityType = GetLeanSecurityType(parts);
            var market = GetLeanMarket(parts);

            if (parts.Length == 3)
            {
                var ticker = parts[0];

                if (securityType == SecurityType.Forex)
                {
                    ticker += "USD";
                }
                else if (securityType == SecurityType.Future)
                {
                    var properties = SymbolRepresentation.ParseFutureTicker(ticker);
                    return Symbol.CreateFuture(
                        properties.Underlying,
                        market,
                        new DateTime(properties.ExpirationYearShort + 2000, properties.ExpirationMonth, properties.ExpirationDay));
                }

                return Symbol.Create(ticker, securityType, market);
            }
            if (parts.Length > 3)
            {
                var underlying = parts[0];
                var right = parts[3] == "C" ? OptionRight.Call : OptionRight.Put;
                var strike = Convert.ToDecimal(parts[4], CultureInfo.InvariantCulture);
                var expiry = DateTime.ParseExact(parts[2], "MM/dd/yy", CultureInfo.InvariantCulture);
                return Symbol.CreateOption(underlying, Market.USA, OptionStyle.American, right, strike, expiry);
            }

            throw new ArgumentException("Invalid brokerage symbol: " + brokerageSymbol);
        }

        public Dictionary<string, BloombergMappingInfo> MappingInfo { get; }

        private string GetBloombergTopicName(Symbol symbol)
        {
            if (symbol.SecurityType == SecurityType.Future)
            {
                if (!_mapLeanToBloomberg.TryGetValue(symbol, out var ticker))
                {
                    ticker = BuildFutureTickerFromLeanSymbol(symbol);
                }

                return ticker;
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
            var entry = MappingInfo.FirstOrDefault(x => x.Value.Underlying == symbol.Value);
            if (entry.Value == null)
            {
                throw new Exception($"Lean ticker not found: {symbol.Value}");
            }
                
            var ticker = $"{entry.Key}{entry.Value.RootLookupSuffix} {entry.Value.TickerSuffix}";

            _mapBloombergToLean.Add(ticker, symbol);
            _mapLeanToBloomberg.Add(symbol, ticker);

            return ticker;
        }

        private Symbol BuildLeanSymbolFromFutureTicker(string brokerageSymbol)
        {
            if (brokerageSymbol.Length < 2)
            {
                throw new Exception("Future ticker length must be at least 2.");
            }

            var rootTicker = brokerageSymbol.Substring(0, 2);

            if (!MappingInfo.TryGetValue(rootTicker, out var info))
            {
                throw new Exception($"Root ticker not found: {rootTicker}");
            }

            var parts = brokerageSymbol.Substring(2).Split(' ');

            Symbol symbol;
            if (parts[0] == info.RootLookupSuffix)
            {
                // canonical future symbol
                symbol = Symbol.Create(info.Underlying, SecurityType.Future, info.Market, $"/{info.Underlying}");
            }
            else
            {
                // future contract
                var ticker = info.Underlying + parts[0];

                var newTicker = ticker;
                var properties = SymbolRepresentation.ParseFutureTicker(newTicker);
                var expiryFunc = FuturesExpiryFunctions.FuturesExpiryFunction(info.Underlying);

                var year = DateTime.UtcNow.Year;
                year = year - year % (properties.ExpirationYearShort < 10 ? 10 : 100) + properties.ExpirationYearShort;

                var expiryDate = expiryFunc(new DateTime(year, properties.ExpirationMonth, properties.ExpirationDay));
                symbol = Symbol.CreateFuture(info.Underlying, info.Market, expiryDate);
            }

            _mapBloombergToLean.Add(brokerageSymbol, symbol);

            if (!_mapLeanToBloomberg.ContainsKey(symbol))
            {
                _mapLeanToBloomberg.Add(symbol, brokerageSymbol);
            }

            return symbol;
        }


        private string GetBloombergSymbol(Symbol symbol)
        {
            if (symbol.SecurityType == SecurityType.Forex)
            {
                // TODO: documentation does not mention non-USD fx pairs, needs to be tested

                if (symbol.Value.EndsWith("USD"))
                {
                    return symbol.Value.Substring(0, 3);
                }
                if (symbol.Value.StartsWith("USD"))
                {
                    return symbol.Value.Substring(3);
                }

                throw new NotSupportedException($"Unsupported Forex symbol: {symbol.Value}");
            }

            if (symbol.SecurityType == SecurityType.Option)
            {
                // Equity options: Root Ticker x Exchange Code x Expiry MM/DD/YY (or Expiry M/Y only) x C or P x Strike Price
                return Invariant($"{symbol.Underlying.Value} UO {symbol.ID.Date:MM/dd/yy} {(symbol.ID.OptionRight == OptionRight.Call ? "C" : "P")} {symbol.ID.StrikePrice:F2}");
            }

            return symbol.Value;
        }

        private string GetBloombergMarket(string market, SecurityType securityType)
        {
            if (securityType == SecurityType.Forex)
            {
                return "BVAL";
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

                case SecurityType.Future:
                    // TODO: depends on the underlying which we don't have for now
                    // possible values: Comdty, Index, Curncy, Equity
                    return "Comdty";

                case SecurityType.Option:
                    // only equity options for now
                    return "Equity";

                default:
                    throw new NotSupportedException($"Unsupported security type: {securityType}");
            }
        }

        private SecurityType GetLeanSecurityType(string[] brokerageSymbolParts)
        {
            if (brokerageSymbolParts[1] == "BVAL")
            {
                return SecurityType.Forex;
            }
            if (brokerageSymbolParts[1] == "COMB")
            {
                return SecurityType.Future;
            }
            if (brokerageSymbolParts.Length > 3)
            {
                return SecurityType.Option;
            }
            if (brokerageSymbolParts[1] == "US" && brokerageSymbolParts[2] == "Equity")
            {
                return SecurityType.Equity;
            }

            return SecurityType.Base;
        }

        private string GetLeanMarket(string[] brokerageSymbolParts)
        {
            if (brokerageSymbolParts[1] == "BVAL")
            {
                return Market.FXCM;
            }
            if (brokerageSymbolParts[1] == "COMB")
            {
                return Market.USA;
            }
            if (brokerageSymbolParts[1] == "US")
            {
                return Market.USA;
            }

            return string.Empty;
        }
    }
}
