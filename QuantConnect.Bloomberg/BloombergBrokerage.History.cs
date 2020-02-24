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

namespace QuantConnect.Bloomberg
{
    public partial class BloombergBrokerage
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
                    // historical daily quotes are not supported natively by Bloomberg so we aggregate from hourly data
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
                    // second resolution is not supported natively by Bloomberg so we aggregate from tick data
                    return historyRequest.TickType == TickType.Quote
                        ? AggregateTicksToQuoteBars(GetIntradayTickData(historyRequest), historyRequest.Symbol, Time.OneSecond)
                        : AggregateTicksToTradeBars(GetIntradayTickData(historyRequest), historyRequest.Symbol, Time.OneSecond);

                case Resolution.Tick:
                    return GetIntradayTickData(historyRequest);

                default:
                    Log.Error($"Unsupported resolution: {historyRequest.Resolution}");
                    return Enumerable.Empty<BaseData>();
            }
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
            // Bloomberg intraday bar requests only allow a single event type:
            // for TickType.Trade - single request for "TRADE"
            // for TickType.Quote - two requests for "BID" and "ASK" with result sets joined
            if (historyRequest.TickType == TickType.Quote)
            {
                var historyBid = GetIntradayBarData(historyRequest, "BID");
                var historyAsk = GetIntradayBarData(historyRequest, "ASK");

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

            return GetIntradayBarData(historyRequest, "TRADE");
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

            var startDate = historyRequest.StartTimeUtc
                .ConvertFromUtc(historyRequest.ExchangeHours.TimeZone).Date;
            var endDate = historyRequest.EndTimeUtc
                .ConvertFromUtc(historyRequest.ExchangeHours.TimeZone).Date;
            request.Append(BloombergNames.Securities, _symbolMapper.GetBrokerageSymbol(historyRequest.Symbol));

            var fields = request.GetElement(BloombergNames.Fields);
            fields.AppendValue(BloombergNames.Open.ToString());
            fields.AppendValue(BloombergNames.High.ToString());
            fields.AppendValue(BloombergNames.Low.ToString());
            fields.AppendValue(BloombergNames.Close.ToString());
            fields.AppendValue(BloombergNames.Volume.ToString());

            request.Set("periodicitySelection", "DAILY");
            request.Set("startDate", startDate.ToString("yyyyMMdd"));
            request.Set("endDate", endDate.ToString("yyyyMMdd"));

            return RequestAndParse(request, BloombergNames.SecurityData, BloombergNames.FieldData, row => CreateTradeBar(historyRequest, row, Time.OneDay));
        }

        private static TradeBar CreateTradeBar(HistoryRequest request, Element row, TimeSpan period)
        {
            var bar = new TradeBar {Symbol = request.Symbol, Period = period};
            if (row.HasElement(BloombergNames.Date))
            {
                var date = row[BloombergNames.Date].GetValueAsDate();
                bar.Time = new DateTime(date.Year, date.Month, date.DayOfMonth);
                bar.Period = Time.OneDay;
            }
            else if (row.HasElement("time"))
            {
                var time = row["time"].GetValueAsDatetime();
                var barTime = new DateTime(time.Year, time.Month, time.DayOfMonth, time.Hour, time.Minute, time.Second).ConvertFromUtc(request.ExchangeHours.TimeZone);
                bar.Time = barTime;
            }
            else
            {
                throw new Exception($"Date or time was not received [symbol:{request.Symbol},bbg-row:{row}]");
            }

            if (TryReadDecimal(row, BloombergNames.Open, out var open))
            {
                bar.Open = open;
            }

            if (TryReadDecimal(row, BloombergNames.High, out var high))
            {
                bar.High = high;
            }

            if (TryReadDecimal(row, BloombergNames.Low, out var low))
            {
                bar.Low = low;
            }

            if (TryReadDecimal(row, BloombergNames.Close, out var close))
            {
                bar.Close = close;
            }

            if (TryReadDecimal(row, BloombergNames.Volume, out var volume))
            {
                bar.Volume = volume;
            }

            return bar;
        }

        private static bool TryReadDecimal(Element element, Name name, out decimal result)
        {
            if (element.HasElement(name))
            {
                result = Convert.ToDecimal(element.GetElementAsFloat64(name));
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

        private IEnumerable<TradeBar> GetIntradayBarData(HistoryRequest historyRequest, string eventType)
        {
            var request = _serviceReferenceData.CreateRequest("IntradayBarRequest");

            var period = historyRequest.Resolution.ToTimeSpan();

            var startDateTime = historyRequest.StartTimeUtc
                .ConvertFromUtc(historyRequest.ExchangeHours.TimeZone);
            var endDateTime = historyRequest.EndTimeUtc
                .ConvertFromUtc(historyRequest.ExchangeHours.TimeZone);

            request.Set("security", _symbolMapper.GetBrokerageSymbol(historyRequest.Symbol));
            request.Set("eventType", eventType);
            request.Set("interval", GetIntervalMinutes(historyRequest.Resolution));
            request.Set("startDateTime", new Datetime(startDateTime.RoundDown(period)));
            request.Set("endDateTime", new Datetime(endDateTime.RoundDown(period)));

            return RequestAndParse(request, BloombergNames.BarData, BloombergNames.BarTickData, row => CreateTradeBar(historyRequest, row, period));
        }

        // TODO: with real API - use reset event to wait for async responses received in OnBloombergEvent
        private IEnumerable<T> RequestAndParse<T>(Request request, Name arrayName, Name arrayItemName, Func<Element, T> createFunc)
        {
            var correlationId = GetNewCorrelationId();
            _sessionReferenceData.SendRequest(request, correlationId);
            var responsePending = true;
            var bars = new List<T>();
            while (responsePending)
            {
                var eventObj = _sessionReferenceData.NextEvent();

                var msg = eventObj.GetMessages().FirstOrDefault(f => f.CorrelationIDs.Contains(correlationId));
                if (msg == default)
                {
                    continue;
                }

                responsePending = eventObj.Type != Event.EventType.RESPONSE;
                if (msg.HasElement(BloombergNames.ResponseError))
                {
                    FireBrokerMessage(new BrokerageMessageEvent(BrokerageMessageType.Error, -1, "Error: " + msg));
                    continue;
                }

                var rows = msg.AsElement[arrayName][arrayItemName];
                for (var i = 0; i < rows.NumValues; i++)
                {
                    var row = rows.GetValueAsElement(i);
                    bars.Add(createFunc(row));
                }
            }

            return bars;
        }

        private IEnumerable<Tick> GetIntradayTickData(HistoryRequest historyRequest)
        {
            var request = _serviceReferenceData.CreateRequest("IntradayTickRequest");

            var startDateTime = historyRequest.StartTimeUtc
                .ConvertFromUtc(historyRequest.ExchangeHours.TimeZone);
            var endDateTime = historyRequest.EndTimeUtc
                .ConvertFromUtc(historyRequest.ExchangeHours.TimeZone);

            request.Set("security", _symbolMapper.GetBrokerageSymbol(historyRequest.Symbol));

            if (historyRequest.TickType == TickType.Trade)
            {
                request.Append("eventTypes", "TRADE");
            }
            else if (historyRequest.TickType == TickType.Quote)
            {
                request.Append("eventTypes", "BID");
                request.Append("eventTypes", "ASK");
            }
            else
            {
                throw new NotSupportedException($"GetIntradayTickData(): unsupported tick type: {historyRequest.TickType}");
            }

            request.Set("startDateTime", new Datetime(startDateTime));
            request.Set("endDateTime", new Datetime(endDateTime));
            request.Set("includeConditionCodes", true);

            var correlationId = GetNewCorrelationId();
            _sessionReferenceData.SendRequest(request, correlationId);

            // TODO: with real API - use reset event to wait for async responses received in OnBloombergEvent

            while (true)
            {
                var eventObj = _sessionReferenceData.NextEvent();
                foreach (var msg in eventObj)
                {
                    if (Equals(msg.CorrelationID, correlationId))
                    {
                        var rows = msg["tickData"]["tickData"];

                        var bidPrice = 0m;
                        var askPrice = 0m;
                        var bidSize = 0m;
                        var askSize = 0m;
                        for (var i = 0; i < rows.NumValues; i++)
                        {
                            var row = rows.GetValueAsElement(i);
                            var time = row["time"].GetValueAsDatetime();

                            var tickTime = new DateTime(time.Year, time.Month, time.DayOfMonth, time.Hour, time.Minute, time.Second)
                                .ConvertFromUtc(historyRequest.ExchangeHours.TimeZone);
                            var type = row.GetElementAsString("type");
                            var value = Convert.ToDecimal(row.GetElementAsFloat64("value"));
                            var size = Convert.ToDecimal(row.GetElementAsFloat64("size"));

                            if (type == "TRADE")
                            {
                                yield return new Tick
                                {
                                    Symbol = historyRequest.Symbol,
                                    Time = tickTime,
                                    TickType = TickType.Trade,
                                    Value = value,
                                    Quantity = size
                                };
                            }
                            else if (type == "BID")
                            {
                                bidPrice = value;
                                bidSize = size;

                                if (bidPrice > 0 && askPrice > 0)
                                {
                                    yield return new Tick
                                    {
                                        Symbol = historyRequest.Symbol,
                                        Time = tickTime,
                                        TickType = TickType.Quote,
                                        BidPrice = bidPrice,
                                        AskPrice = askPrice,
                                        BidSize = bidSize,
                                        AskSize = askSize
                                    };
                                }
                            }
                            else if (type == "ASK")
                            {
                                askPrice = value;
                                askSize = size;

                                if (bidPrice > 0 && askPrice > 0)
                                {
                                    yield return new Tick
                                    {
                                        Symbol = historyRequest.Symbol,
                                        Time = tickTime,
                                        TickType = TickType.Quote,
                                        BidPrice = bidPrice,
                                        AskPrice = askPrice,
                                        BidSize = bidSize,
                                        AskSize = askSize
                                    };
                                }
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
