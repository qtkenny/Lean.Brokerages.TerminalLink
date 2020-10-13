/*
* QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
* Lean Algorithmic Trading Engine v2.2 Copyright 2015 QuantConnect Corporation.
*/

using System;
using System.Collections.Generic;
using System.Linq;
using Bloomberglp.Blpapi;
using QuantConnect.Brokerages;
using QuantConnect.Interfaces;
using QuantConnect.Logging;

namespace QuantConnect.Bloomberg
{
    /// <summary>
    /// An implementation of <see cref="IDataQueueUniverseProvider"/> for Bloomberg
    /// </summary>
    public partial class BloombergBrokerage : IDataQueueUniverseProvider
    {
        /// <summary>
        /// Method returns a collection of Symbols that are available at the broker.
        /// </summary>
        /// <param name="lookupName">String representing the name to lookup</param>
        /// <param name="securityType">Expected security type of the returned symbols (if any)</param>
        /// <param name="includeExpired">Include expired contracts</param>
        /// <param name="securityCurrency">Expected security currency(if any)</param>
        /// <param name="securityExchange">Expected security exchange name(if any)</param>
        /// <returns></returns>
        public IEnumerable<Symbol> LookupSymbols(string lookupName, SecurityType securityType, bool includeExpired, string securityCurrency = null, string securityExchange = null)
        {
            if (string.IsNullOrWhiteSpace(lookupName))
            {
                throw new ArgumentException($"Only {nameof(LookupSymbols)} providing '{nameof(lookupName)}' are supported.");
            }

            if (securityType != SecurityType.Option && securityType != SecurityType.Future)
            {
                throw new NotSupportedException($"Only {SecurityType.Option} and {SecurityType.Future} security types are supported.");
            }

            Log.Trace($"BloombergBrokerage.LookupSymbols(): Requesting symbol list for {lookupName} ...");
            var market = _symbolMapper.GetMarket(lookupName) ?? Market.USA;
            var canonicalSymbol = Symbol.Create(lookupName, securityType, market);
            var symbols = GetChain(canonicalSymbol, securityType, includeExpired).ToList();

            Log.Trace($"BloombergBrokerage.LookupSymbols(): Returning {symbols.Count} contract(s) for {lookupName}");

            return symbols;
        }

        /// <summary>
        /// Returns whether the time can be advanced or not.
        /// </summary>
        /// <param name="securityType">The security type</param>
        /// <returns>true if the time can be advanced</returns>
        public bool CanAdvanceTime(SecurityType securityType)
        {
            return true;
        }

        private IEnumerable<Symbol> GetChain(Symbol canonicalSymbol, SecurityType securityType, bool includeExpired)
        {
            var chain = _symbolMapper.GetManualChain(canonicalSymbol);
            if (chain == null || chain.Length == 0)
            {
                chain = GetChainFromBloomberg(canonicalSymbol, securityType, includeExpired).ToArray();
            }

            foreach (var contractTicker in chain)
            {
                Log.Trace($"BloombergBrokerage.GetChain(): BBG contract ticker: {contractTicker}");

                var contractSymbol = _symbolMapper.GetLeanSymbol(contractTicker, securityType);

                Log.Trace($"BloombergBrokerage.GetChain(): LEAN symbol: {contractSymbol.Value} [{contractSymbol}]");

                yield return contractSymbol;
            }
        }

        private IEnumerable<string> GetChainFromBloomberg(Symbol canonicalSymbol, SecurityType securityType, bool includeExpired)
        {
            var ticker = _symbolMapper.GetBrokerageSymbol(canonicalSymbol);
            var chainFieldName = securityType == SecurityType.Future ? BloombergFieldNames.FuturesChain : BloombergFieldNames.OptionsChain;

            var request = _serviceReferenceData.CreateRequest("ReferenceDataRequest");

            request.Append(BloombergNames.Securities, ticker);

            request.Append(BloombergNames.Fields, chainFieldName);
            request.Append(BloombergNames.Fields, BloombergNames.Ticker.ToString());
            request.Append(BloombergNames.Fields, BloombergNames.CurrentGenericFuturesTicker.ToString());

            var overrides = request.GetElement("overrides");

            var element = overrides.AppendElement();
            element.SetElement("fieldId", "INCLUDE_EXPIRED_CONTRACTS");
            element.SetElement("value", includeExpired ? "Y" : "N");

            element = overrides.AppendElement();
            element.SetElement("fieldId", "CHAIN_POINTS_OVRD");
            element.SetElement("value", 50000);

            element = overrides.AppendElement();
            element.SetElement("fieldId", "CHAIN_EXP_DT_OVRD");
            element.SetElement("value", "ALL");

            var responses = _sessionReferenceData.SendRequestSynchronous(request);

            foreach (var msg in responses)
            {
                if (msg.IsFailed())
                {
                    var requestFailure = new BloombergRequestFailure(msg);
                    var errorMessage = $"Unable to obtain chain for '{ticker}': Request failed - reason: {requestFailure}";
                    FireBrokerMessage(new BrokerageMessageEvent(BrokerageMessageType.Warning, requestFailure.ErrorCode, errorMessage));
                    yield break;
                }

                // Security data is an array.
                var securityData = msg.AsElement[BloombergNames.SecurityData];
                for (var i = 0; i < securityData.NumValues; i++)
                {
                    var securityItem = (Element) securityData.GetValue(i);
                    if (securityItem.HasElement("securityError"))
                    {
                        var error = securityItem["securityError"];
                        var message = error["message"];
                        var errorMessage = $"Unable to obtain chain for '{ticker}': {message}";
                        FireBrokerMessage(new BrokerageMessageEvent(BrokerageMessageType.Warning, -1, errorMessage));
                        yield break;
                    }

                    var fieldData = securityItem[BloombergNames.FieldData];
                    if (!fieldData.HasElement(chainFieldName, true) || 
                        !(fieldData.HasElement(BloombergNames.CurrentGenericFuturesTicker) || fieldData.HasElement(BloombergNames.Ticker)))
                    {
                        continue;
                    }

                    string firstContract;

                    switch (securityType)
                    {
                        case SecurityType.Future:
                            firstContract = fieldData.HasElement(BloombergNames.CurrentGenericFuturesTicker)
                                ? fieldData.GetElementAsString(BloombergNames.CurrentGenericFuturesTicker)
                                : fieldData.GetElementAsString(BloombergNames.Ticker);
                            break;
                        case SecurityType.Option:
                            firstContract = fieldData.GetElementAsString(BloombergNames.Ticker);
                            break;
                        default:
                            throw new ArgumentException(nameof(securityType));
                    }

                    var chainTickers = fieldData.GetElement(chainFieldName);
                    var hasFoundFirstContract = !_startAtActive;
                    for (var index = 0; index < chainTickers.NumValues; index++)
                    {
                        var chainTicker = chainTickers.GetValueAsElement(index);
                        var contractTicker = chainTicker.GetElementAsString("Security Description");
                        hasFoundFirstContract |= contractTicker.StartsWith(firstContract);
                        if (hasFoundFirstContract)
                        {
                            yield return contractTicker;
                        }
                    }
                }
            }
        }
    }
}