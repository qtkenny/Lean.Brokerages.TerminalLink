/*
* QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
* Lean Algorithmic Trading Engine v2.2 Copyright 2015 QuantConnect Corporation.
*/

using System;
using System.Linq;
using Bloomberglp.Blpapi;
using QuantConnect.Logging;
using QuantConnect.Brokerages;
using QuantConnect.Interfaces;
using System.Collections.Generic;

namespace QuantConnect.TerminalLink
{
    /// <summary>
    /// An implementation of <see cref="IDataQueueUniverseProvider"/> for TerminalLink
    /// </summary>
    public partial class TerminalLinkBrokerage : IDataQueueUniverseProvider
    {
        /// <summary>
        /// Method returns a collection of Symbols that are available at the data source.
        /// </summary>
        /// <param name="symbol">Symbol to lookup</param>
        /// <param name="includeExpired">Include expired contracts</param>
        /// <param name="securityCurrency">Expected security currency(if any)</param>
        /// <returns>Enumerable of Symbols, that are associated with the provided Symbol</returns>
        /// <returns></returns>
        public IEnumerable<Symbol> LookupSymbols(Symbol symbol, bool includeExpired, string securityCurrency = null)
        {
            if (symbol == null)
            {
                throw new ArgumentException($"Only {nameof(LookupSymbols)} providing '{nameof(symbol)}' are supported.");
            }

            var securityType = symbol.SecurityType;
            if (securityType != SecurityType.Option && securityType != SecurityType.Future)
            {
                throw new NotSupportedException($"Only {SecurityType.Option} and {SecurityType.Future} security types are supported.");
            }

            var canonical = symbol.Canonical;
            Log.Trace($"TerminalLinkBrokerage.LookupSymbols(): Requesting symbol list for {canonical} ...");
            var symbols = GetChain(canonical, securityType, includeExpired).ToList();
            Log.Trace($"TerminalLinkBrokerage.LookupSymbols(): Returning {symbols.Count} contract(s) for {canonical}");

            return symbols;
        }

        /// <summary>
        /// Returns whether selection can take place or not.
        /// </summary>
        /// <remarks>This is useful to avoid a selection taking place during invalid times, for example IB reset times or when not connected,
        /// because if allowed selection would fail since IB isn't running and would kill the algorithm</remarks>
        /// <returns>True if selection can take place</returns>
        public bool CanPerformSelection()
        {
            return true;
        }

        private IEnumerable<Symbol> GetChain(Symbol canonicalSymbol, SecurityType securityType, bool includeExpired)
        {
            var chain = _symbolMapper.GetManualChain(canonicalSymbol);
            if (chain == null || chain.Length == 0)
            {
                chain = GetChainFromTerminalLink(canonicalSymbol, securityType, includeExpired).ToArray();
            }

            foreach (var contractTicker in chain)
            {
                Log.Trace($"TerminalLinkBrokerage.GetChain(): BBG contract ticker: {contractTicker}");

                var contractSymbol = _symbolMapper.GetLeanSymbol(contractTicker, securityType);

                Log.Trace($"TerminalLinkBrokerage.GetChain(): LEAN symbol: {contractSymbol.Value} [{contractSymbol}]");

                yield return contractSymbol;
            }
        }

        private IEnumerable<string> GetChainFromTerminalLink(Symbol canonicalSymbol, SecurityType securityType, bool includeExpired)
        {
            var ticker = _symbolMapper.GetBrokerageSymbol(canonicalSymbol);
            var chainFieldName = securityType == SecurityType.Future ? TerminalLinkFieldNames.FuturesChain : TerminalLinkFieldNames.OptionsChain;

            var request = _serviceReferenceData.CreateRequest("ReferenceDataRequest");

            request.Append(TerminalLinkNames.Securities, ticker);

            request.Append(TerminalLinkNames.Fields, chainFieldName);
            request.Append(TerminalLinkNames.Fields, TerminalLinkNames.Ticker.ToString());
            request.Append(TerminalLinkNames.Fields, TerminalLinkNames.CurrentGenericFuturesTicker.ToString());

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
                    var requestFailure = new TerminalLinkRequestFailure(msg);
                    var errorMessage = $"Unable to obtain chain for '{ticker}': Request failed - reason: {requestFailure}";
                    FireBrokerMessage(new BrokerageMessageEvent(BrokerageMessageType.Warning, requestFailure.ErrorCode, errorMessage));
                    yield break;
                }

                // Security data is an array.
                var securityData = msg.AsElement[TerminalLinkNames.SecurityData];
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

                    var fieldData = securityItem[TerminalLinkNames.FieldData];
                    if (!fieldData.HasElement(chainFieldName, true) || 
                        !(fieldData.HasElement(TerminalLinkNames.CurrentGenericFuturesTicker) || fieldData.HasElement(TerminalLinkNames.Ticker)))
                    {
                        continue;
                    }

                    string firstContract;

                    switch (securityType)
                    {
                        case SecurityType.Future:
                            firstContract = fieldData.HasElement(TerminalLinkNames.CurrentGenericFuturesTicker)
                                ? fieldData.GetElementAsString(TerminalLinkNames.CurrentGenericFuturesTicker)
                                : fieldData.GetElementAsString(TerminalLinkNames.Ticker);
                            break;
                        case SecurityType.Option:
                            firstContract = fieldData.GetElementAsString(TerminalLinkNames.Ticker);
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