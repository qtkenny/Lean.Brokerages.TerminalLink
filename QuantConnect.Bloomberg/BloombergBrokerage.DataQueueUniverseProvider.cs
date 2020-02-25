/*
* QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
* Lean Algorithmic Trading Engine v2.2 Copyright 2015 QuantConnect Corporation.
*/

using System;
using System.Collections.Generic;
using System.Linq;
using Bloomberglp.Blpapi;
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
        /// <param name="securityCurrency">Expected security currency(if any)</param>
        /// <param name="securityExchange">Expected security exchange name(if any)</param>
        /// <returns></returns>
        public IEnumerable<Symbol> LookupSymbols(string lookupName, SecurityType securityType, string securityCurrency = null, string securityExchange = null)
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

            var canonicalSymbol = Symbol.Create(lookupName, securityType, Market.USA);
            var ticker = _symbolMapper.GetBrokerageSymbol(canonicalSymbol);

            var symbols = GetChain(ticker).ToList();

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

        private IEnumerable<Symbol> GetChain(string ticker)
        {
            var request = _serviceReferenceData.CreateRequest("ReferenceDataRequest");

            request.Append(BloombergNames.Securities, ticker);

            request.Append(BloombergNames.Fields, BloombergFieldNames.FuturesChain);

            var overrides = request.GetElement("overrides");

            var element = overrides.AppendElement();
            element.SetElement("fieldId", "INCLUDE_EXPIRED_CONTRACTS");
            element.SetElement("value", "N");

            element = overrides.AppendElement();
            element.SetElement("fieldId", "CHAIN_POINTS_OVRD");
            element.SetElement("value", 50000);

            element = overrides.AppendElement();
            element.SetElement("fieldId", "CHAIN_EXP_DT_OVRD");
            element.SetElement("value", "ALL");

            var correlationId = GetNewCorrelationId();
            _sessionReferenceData.SendRequest(request, correlationId);

            while (true)
            {
                var eventObj = _sessionReferenceData.NextEvent();
                foreach (var msg in eventObj.Where(m => Equals(m.CorrelationID, correlationId)))
                {
                    // Security data is an array.
                    var securityData = msg.AsElement[BloombergNames.SecurityData];
                    for (var i = 0; i < securityData.NumValues; i++)
                    {
                        var securityItem = (Element) securityData.GetValue(i);
                        if (securityItem.HasElement("securityError"))
                        {
                            var error = securityItem["securityError"];
                            var message = error["message"];
                            Log.Error($"Unable to obtain chain for '{ticker}': {message}");
                            yield break;
                        }

                        var fieldData = securityItem[BloombergNames.FieldData];
                        if (fieldData.HasElement(BloombergFieldNames.FuturesChain, true))
                        {
                            var chainTickers = fieldData.GetElement(BloombergFieldNames.FuturesChain);
                            for (var index = 0; index < chainTickers.NumValues; index++)
                            {
                                var chainTicker = chainTickers.GetValueAsElement(index);
                                var contractTicker = chainTicker.GetElementAsString("Security Description");

                                Log.Trace($"BloombergBrokerage.GetChain(): BBG contract ticker: {contractTicker}");

                                var contractSymbol = _symbolMapper.GetLeanSymbol(contractTicker, SecurityType.Future);

                                Log.Trace($"BloombergBrokerage.GetChain(): LEAN symbol: {contractSymbol.Value} [{contractSymbol}]");

                                yield return contractSymbol;
                            }
                        }
                    }
                }

                if (eventObj.Type == Event.EventType.RESPONSE)
                {
                    yield break;
                }
            }
        }
    }
}