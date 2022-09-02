/*
* QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
* Lean Algorithmic Trading Engine v2.2 Copyright 2015 QuantConnect Corporation.
*/

using System;
using System.Collections.Generic;
using System.Linq;
using Bloomberglp.Blpapi;
using QuantConnect.Brokerages;
using QuantConnect.Data;
using QuantConnect.Data.Market;
using QuantConnect.Logging;

namespace QuantConnect.TerminalLink
{
    public partial class TerminalLinkBrokerage
    {
        /// <summary>
        /// Gets the history for the requested security
        /// </summary>
        /// <param name="historyRequest">The historical data request</param>
        /// <returns>An enumerable of bars covering the span specified in the request</returns>
        public override IEnumerable<BaseData> GetHistory(HistoryRequest historyRequest)
        {
            switch (historyRequest.Resolution)
            {
                case Resolution.Daily:
                    // historical daily quotes are not supported natively by TerminalLink so we aggregate from hourly data
                    if (historyRequest.TickType == TickType.Quote)
                    {
                        var historyRequestHour = new HistoryRequest(
                            historyRequest.StartTimeUtc,
                            historyRequest.EndTimeUtc,
                            historyRequest.DataType,
                            historyRequest.Symbol,
                            Resolution.Hour,
                            historyRequest.ExchangeHours,
                            historyRequest.DataTimeZone,
                            historyRequest.FillForwardResolution,
                            historyRequest.IncludeExtendedMarketHours,
                            historyRequest.IsCustomData,
                            historyRequest.DataNormalizationMode,
                            historyRequest.TickType);
                        return AggregateQuoteBars(GetIntradayBarData(historyRequestHour), historyRequest.Symbol, Time.OneDay);
                    }
                    else
                    {
                        return GetHistoricalData(historyRequest);
                    }

                case Resolution.Hour:
                case Resolution.Minute:
                    return GetIntradayBarData(historyRequest);

                case Resolution.Second:
                    // second resolution is not supported natively by TerminalLink so we aggregate from tick data
                    return historyRequest.TickType == TickType.Quote
                        ? AggregateTicksToQuoteBars(GetIntradayTickData(historyRequest), historyRequest.Symbol, Time.OneSecond)
                        : AggregateTicksToTradeBars(GetIntradayTickData(historyRequest), historyRequest.Symbol, Time.OneSecond);

                case Resolution.Tick:
                    if (historyRequest.TickType == TickType.OpenInterest)
                    {
                        // TerminalLink does not support OpenInterest historical data,
                        // so we just return a single tick containing the current value.
                        return GetOpenInterestTickData(historyRequest);
                    }
                    else
                    {
                        return GetIntradayTickData(historyRequest);
                    }

                default:
                    Log.Error($"Unsupported resolution: {historyRequest.Resolution}");
                    return Enumerable.Empty<BaseData>();
            }
        }

        private IEnumerable<BaseData> GetOpenInterestTickData(HistoryRequest historyRequest)
        {
            var request = _serviceReferenceData.CreateRequest("ReferenceDataRequest");

            request.Append(TerminalLinkNames.Securities, _symbolMapper.GetBrokerageSymbol(historyRequest.Symbol));

            var fields = request.GetElement(TerminalLinkNames.Fields);
            fields.AppendValue(TerminalLinkNames.OpenInterest.ToString());
            fields.AppendValue(TerminalLinkNames.OpenInterestDate.ToString());

            return RequestAndParse(request, TerminalLinkNames.SecurityData, null, TerminalLinkNames.FieldData, row => CreateOpenInterestTick(historyRequest, row));
        }

        private Tick CreateOpenInterestTick(HistoryRequest historyRequest, Element row)
        {
            var tick = new Tick
            {
                Symbol = historyRequest.Symbol,
                TickType = TickType.OpenInterest
            };

            if (row.HasElement(TerminalLinkNames.OpenInterestDate))
            {
                var date = row[TerminalLinkNames.OpenInterestDate].GetValueAsDate();
                tick.Time = new DateTime(date.Year, date.Month, date.DayOfMonth);
                if (TryReadDecimal(row, TerminalLinkNames.OpenInterest, out var openInterest))
                {
                    tick.Value = openInterest;
                }
            }
            else
            {
                Log.Error("OpenInterestDate was not received [symbol:{0},bbg-row:{1}]", historyRequest.Symbol, row);
                return null;
            }

            return tick;
        }

        private static IEnumerable<BaseData> AggregateQuoteBars(IEnumerable<BaseData> bars, Symbol symbol, TimeSpan period)
        {
            return
            (from b in bars
                let bar = (QuoteBar)b
                group bar by bar.Time.RoundDown(period)
                into g
                select (BaseData)new QuoteBar
                {
                    Symbol = symbol,
                    Time = g.Key,
                    Bid = new Bar
                    {
                        Open = g.First().Bid.Open,
                        High = g.Max(b => b.Bid.High),
                        Low = g.Min(b => b.Bid.Low),
                        Close = g.Last().Bid.Close
                    },
                    LastBidSize = g.Last().LastBidSize,
                    Ask = new Bar
                    {
                        Open = g.First().Ask.Open,
                        High = g.Max(b => b.Ask.High),
                        Low = g.Min(b => b.Ask.Low),
                        Close = g.Last().Ask.Close
                    },
                    LastAskSize = g.Last().LastAskSize,
                    Value = g.Last().Close,
                    Period = period
                });
        }

        private IEnumerable<BaseData> GetIntradayBarData(HistoryRequest historyRequest)
        {
            // TerminalLink intraday bar requests only allow a single event type:
            // for TickType.Trade - single request for "TRADE"
            // for TickType.Quote - two requests for "BID" and "ASK" with result sets joined
            if (historyRequest.TickType == TickType.Quote)
            {
                var historyBid = GetIntradayTradeBars(historyRequest, "BID");
                var historyAsk = GetIntradayTradeBars(historyRequest, "ASK");

                return historyBid.Join(historyAsk,
                    bid => bid.Time,
                    ask => ask.Time,
                    (bid, ask) => new QuoteBar(
                        bid.Time,
                        bid.Symbol,
                        new Bar(bid.Open, bid.High, bid.Low, bid.Close),
                        bid.Volume,
                        new Bar(ask.Open, ask.High, ask.Low, ask.Close),
                        ask.Volume,
                        bid.Period));
            }

            return GetIntradayTradeBars(historyRequest, "TRADE");
        }

        private static IEnumerable<BaseData> AggregateTicksToTradeBars(IEnumerable<Tick> ticks, Symbol symbol, TimeSpan resolutionTimeSpan)
        {
            return
                from t in ticks
                group t by t.Time.RoundDown(resolutionTimeSpan)
                into g
                select new TradeBar
                {
                    Symbol = symbol,
                    Time = g.Key,
                    Period = resolutionTimeSpan,
                    Open = g.First().LastPrice,
                    High = g.Max(t => t.LastPrice),
                    Low = g.Min(t => t.LastPrice),
                    Close = g.Last().LastPrice
                };
        }

        private static IEnumerable<BaseData> AggregateTicksToQuoteBars(IEnumerable<Tick> ticks, Symbol symbol, TimeSpan resolutionTimeSpan)
        {
            return
                from t in ticks
                group t by t.Time.RoundDown(resolutionTimeSpan)
                into g
                select new QuoteBar
                {
                    Symbol = symbol,
                    Time = g.Key,
                    Period = resolutionTimeSpan,
                    Bid = new Bar
                    {
                        Open = g.First().BidPrice,
                        High = g.Max(b => b.BidPrice),
                        Low = g.Min(b => b.BidPrice),
                        Close = g.Last().BidPrice
                    },
                    Ask = new Bar
                    {
                        Open = g.First().AskPrice,
                        High = g.Max(b => b.AskPrice),
                        Low = g.Min(b => b.AskPrice),
                        Close = g.Last().AskPrice
                    }
                };
        }

        private IEnumerable<BaseData> GetHistoricalData(HistoryRequest historyRequest)
        {
            var request = _serviceReferenceData.CreateRequest("HistoricalDataRequest");
            var startDate = historyRequest.StartTimeUtc.ConvertFromUtc(UserTimeZone).Date;
            var endDate = historyRequest.EndTimeUtc.ConvertFromUtc(UserTimeZone).Date;
            request.Append(TerminalLinkNames.Securities, _symbolMapper.GetBrokerageSymbol(historyRequest.Symbol));

            var fields = request.GetElement(TerminalLinkNames.Fields);
            fields.AppendValue(TerminalLinkNames.OpenHistorical.ToString());
            fields.AppendValue(TerminalLinkNames.HighHistorical.ToString());
            fields.AppendValue(TerminalLinkNames.LowHistorical.ToString());
            fields.AppendValue(TerminalLinkNames.TerminalLinkClosePrice.ToString());
            fields.AppendValue(TerminalLinkNames.PxLast.ToString());
            fields.AppendValue(TerminalLinkNames.Volume.ToString());

            request.Set("periodicitySelection", "DAILY");
            request.Set("startDate", startDate.ToString("yyyyMMdd"));
            request.Set("endDate", endDate.ToString("yyyyMMdd"));

            return RequestAndParse(request, TerminalLinkNames.SecurityData, TerminalLinkNames.FieldData, null, row => CreateTradeBar(historyRequest, row, Time.OneDay));
        }

        private TradeBar CreateTradeBar(HistoryRequest request, Element row, TimeSpan period)
        {
            var bar = new TradeBar {Symbol = request.Symbol, Period = period};
            if (row.HasElement(TerminalLinkNames.Date))
            {
                var date = row[TerminalLinkNames.Date].GetValueAsDate();
                bar.Time = new DateTime(date.Year, date.Month, date.DayOfMonth);
                bar.Period = Time.OneDay;
            }
            else if (row.HasElement(TerminalLinkNames.Time))
            {
                var time = row[TerminalLinkNames.Time].GetValueAsDatetime();
                var barTime = new DateTime(time.Year, time.Month, time.DayOfMonth, time.Hour, time.Minute, time.Second).ConvertTo(UserTimeZone, request.ExchangeHours.TimeZone);
                bar.Time = barTime;
            }
            else
            {
                throw new Exception($"Date or time was not received [symbol:{request.Symbol},bbg-row:{row}]");
            }

            if (period == Time.OneDay)
            {
                if (TryReadDecimal(row, TerminalLinkNames.OpenHistorical, out var open))
                {
                    bar.Open = open;
                }

                if (TryReadDecimal(row, TerminalLinkNames.HighHistorical, out var high))
                {
                    bar.High = high;
                }

                if (TryReadDecimal(row, TerminalLinkNames.LowHistorical, out var low))
                {
                    bar.Low = low;
                }

                if (TryReadDecimal(row, TerminalLinkNames.TerminalLinkClosePrice, out var close))
                {
                    bar.Close = close;
                }
                else if (TryReadDecimal(row, TerminalLinkNames.PxLast, out close))
                {
                    bar.Close = close;
                }

                if (TryReadDecimal(row, TerminalLinkNames.Volume, out var volume))
                {
                    bar.Volume = volume;
                }
            }
            else
            {
                if (TryReadDecimal(row, TerminalLinkNames.OpenIntraday, out var open))
                {
                    bar.Open = open;
                }

                if (TryReadDecimal(row, TerminalLinkNames.HighIntraday, out var high))
                {
                    bar.High = high;
                }

                if (TryReadDecimal(row, TerminalLinkNames.LowIntraday, out var low))
                {
                    bar.Low = low;
                }

                if (TryReadDecimal(row, TerminalLinkNames.CloseIntraday, out var close))
                {
                    bar.Close = close;
                }
            }

            return bar;
        }

        private static bool TryReadDecimal(Element element, Name name, out decimal result)
        {
            if (element.HasElement(name, true))
            {
                result = new decimal(element.GetElementAsFloat64(name));
                return true;
            }

            result = decimal.Zero;
            return false;
        }

        private static int GetIntervalMinutes(Resolution resolution)
        {
            switch (resolution)
            {
                case Resolution.Hour:
                    return 60;

                case Resolution.Minute:
                    return 1;

                default:
                    throw new NotSupportedException($"GetIntervalMinutes(): resolution not supported: {resolution}");
            }
        }

        private IEnumerable<TradeBar> GetIntradayTradeBars(HistoryRequest historyRequest, string eventType)
        {
            var request = _serviceReferenceData.CreateRequest("IntradayBarRequest");
            var period = historyRequest.Resolution.ToTimeSpan();
            var startDateTime = historyRequest.StartTimeUtc.ConvertFromUtc(UserTimeZone);
            var endDateTime = historyRequest.EndTimeUtc.ConvertFromUtc(UserTimeZone);

            request.Set("security", _symbolMapper.GetBrokerageSymbol(historyRequest.Symbol));
            request.Set(TerminalLinkNames.EventType, eventType);
            request.Set("interval", GetIntervalMinutes(historyRequest.Resolution));
            request.Set(TerminalLinkNames.StartDateTime, new Datetime(startDateTime.RoundDown(period)));
            request.Set(TerminalLinkNames.EndDateTime, new Datetime(endDateTime.RoundDown(period)));

            return RequestAndParse(request, TerminalLinkNames.BarData, TerminalLinkNames.BarTickData, null, row => CreateTradeBar(historyRequest, row, period));
        }

        private IReadOnlyCollection<T> RequestAndParse<T>(Request request, Name arrayName, Name arrayItemName, Name childElementName, Func<Element, T> createFunc)
        {
            var bars = new List<T>();

            var responses = _sessionReferenceData.SendRequestSynchronous(request);

            foreach (var msg in responses)
            {
                if (msg.IsFailed())
                {
                    var requestFailure = new TerminalLinkRequestFailure(msg);
                    var errorMessage = $"Request Failed: '{msg.MessageType}' - {requestFailure}";
                    FireBrokerMessage(new BrokerageMessageEvent(BrokerageMessageType.Warning, requestFailure.ErrorCode, errorMessage));
                    continue;
                }

                if (msg.HasElement(TerminalLinkNames.ResponseError))
                {
                    FireBrokerMessage(new BrokerageMessageEvent(BrokerageMessageType.Warning, -1, "Error: " + msg));
                    continue;
                }

                if (!msg.HasElement(arrayName))
                {
                    Log.Error("Required element '{0}' was not found in message: {1}", arrayName, msg);
                    continue;
                }

                var rows = msg.AsElement[arrayName];
                if (arrayItemName != null)
                {
                    if (!rows.HasElement(arrayItemName))
                    {
                        Log.Error("Required element '{0}' was not found in message: {1}", arrayName, msg);
                        continue;
                    }

                    rows = rows.GetElement(arrayItemName);
                }

                for (var i = 0; i < rows.NumValues; i++)
                {
                    var row = rows.GetValueAsElement(i);
                    if (childElementName != null)
                    {
                        if (!row.HasElement(childElementName))
                        {
                            Log.Error("Required child '{0}' within array item was not found: {2}", childElementName, msg);
                            break;
                        }

                        row = row.GetElement(childElementName);
                    }

                    var result = createFunc(row);
                    if (result != null)
                    {
                        bars.Add(result);
                    }
                }
            }

            return bars;
        }

        private IEnumerable<Tick> GetIntradayTickData(HistoryRequest historyRequest)
        {
            var request = _serviceReferenceData.CreateRequest("IntradayTickRequest");
            var startDateTime = historyRequest.StartTimeUtc.ConvertFromUtc(UserTimeZone);
            var endDateTime = historyRequest.EndTimeUtc.ConvertFromUtc(UserTimeZone);

            request.Set(TerminalLinkNames.Security, _symbolMapper.GetBrokerageSymbol(historyRequest.Symbol));

            switch (historyRequest.TickType) {
                case TickType.Trade: request.Append(TerminalLinkNames.EventTypes, TerminalLinkNames.Trade);
                    break;
                case TickType.Quote:
                    request.Append(TerminalLinkNames.EventTypes, TerminalLinkNames.BestBid);
                    request.Append(TerminalLinkNames.EventTypes, TerminalLinkNames.BestAsk);

                    break;
                default: throw new NotSupportedException($"GetIntradayTickData(): unsupported tick type: {historyRequest.TickType}");
            }

            request.Set(TerminalLinkNames.StartDateTime, new Datetime(startDateTime));
            request.Set(TerminalLinkNames.EndDateTime, new Datetime(endDateTime));
            request.Set("includeConditionCodes", true);

            var tickData = RequestAndParse(request, TerminalLinkNames.TickData, TerminalLinkNames.TickData, null, row => CreateTick(historyRequest, row));
            if (tickData.Count == 0 || historyRequest.TickType == TickType.Trade)
            {
                return tickData;
            }

            var results = new List<Tick>(tickData.Count - 1);
            var previousTick = tickData.First();
            foreach (var tick in tickData.Skip(1))
            {
                // Any fields that aren't set on this tick update, take the bid/ask values from the previous one.
                if (tick.BidPrice == decimal.Zero)
                {
                    tick.BidPrice = previousTick.BidPrice;
                }

                if (tick.BidSize == decimal.Zero)
                {
                    tick.BidSize = previousTick.BidSize;
                }

                if (tick.AskPrice == decimal.Zero)
                {
                    tick.AskPrice = previousTick.AskPrice;
                }

                if (tick.AskSize == decimal.Zero)
                {
                    tick.AskSize = previousTick.AskSize;
                }

                previousTick = tick;
                if (tick.BidSize != decimal.Zero && tick.BidPrice != decimal.Zero && tick.AskSize != decimal.Zero && tick.AskPrice != decimal.Zero)
                {
                    results.Add(tick);
                }
            }

            return results;
        }

        private Tick CreateTick(HistoryRequest historyRequest, Element row)
        {
            var time = row[TerminalLinkNames.Time].GetValueAsDatetime();
            var tickTime = time.ToSystemDateTime().ConvertTo(UserTimeZone, historyRequest.ExchangeHours.TimeZone);
            var type = row.GetElementAsString(TerminalLinkNames.Type);
            var value = Convert.ToDecimal(row.GetElementAsFloat64(TerminalLinkNames.Value));
            var size = Convert.ToDecimal(row.GetElementAsFloat64(TerminalLinkNames.Size));

            if (type == TerminalLinkNames.Trade.ToString())
            {
                return new Tick {Symbol = historyRequest.Symbol, Time = tickTime, TickType = TickType.Trade, Value = value, Quantity = size};
            }

            if (type == TerminalLinkNames.BestBid.ToString())
            {
                return new Tick {Symbol = historyRequest.Symbol, Time = tickTime, TickType = TickType.Quote, BidPrice = value, BidSize = size, };
            }

            if (type == TerminalLinkNames.BestAsk.ToString())
            {
                return new Tick {Symbol = historyRequest.Symbol, Time = tickTime, TickType = TickType.Quote, AskPrice = value, AskSize = size};
            }

            throw new Exception($"Unknown tick type: {type} [row:{row}]");
        }
    }
}
