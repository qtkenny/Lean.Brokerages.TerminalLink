/*
* QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
* Lean Algorithmic Trading Engine v2.2 Copyright 2015 QuantConnect Corporation.
*/

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Moq;
using NodaTime;
using NUnit.Framework;
using QuantConnect.TerminalLink;
using QuantConnect.Brokerages;
using QuantConnect.Configuration;
using QuantConnect.Data;
using QuantConnect.Data.Market;
using QuantConnect.Lean.Engine.DataFeeds;
using QuantConnect.Logging;
using QuantConnect.Orders;
using QuantConnect.Securities;
using DateTime = System.DateTime;
using Environment = QuantConnect.TerminalLink.Environment;
using HistoryRequest = QuantConnect.Data.HistoryRequest;
using LimitOrder = QuantConnect.Orders.LimitOrder;
using Order = QuantConnect.Orders.Order;
using OrderType = QuantConnect.Orders.OrderType;
using TimeInForce = QuantConnect.Orders.TimeInForce;

namespace QuantConnect.TerminalLinkTests
{
    [TestFixture]
    [Ignore("Broker tests should be executed manually")]
    [Timeout(60000)]
    public class TerminalLinkBrokerageIntegrationTests
    {
        private const int OneDay = 60 * 24;
        private static readonly ConsoleLogHandler FixtureLogHandler = new ConsoleLogHandler();
        private static readonly Action<Order, int> OrderIdSetter;
        private static readonly Symbol TestSymbol = Symbol.Empty;
        private static readonly Mock<IOrderProvider> MockOrderProvider = new Mock<IOrderProvider>();
        private static readonly Mock<TerminalLinkSymbolMapper> MockTerminalLinkSymbolMapper = new Mock<TerminalLinkSymbolMapper>("integration-terminal-link-symbol-map.json") {CallBase = true};
        private static TerminalLinkBrokerage _underTest;
        private ConsoleLogHandler _singleTestLogHandler;

        static TerminalLinkBrokerageIntegrationTests()
        {
            Log.LogHandler = FixtureLogHandler;
            Config.SetConfigurationFile("../../integration-config.json");
            Config.Reset();

            // Work out the order
            var propertyInfo = typeof(LimitOrder).GetProperty(nameof(Order.Id), BindingFlags.Public | BindingFlags.Instance);
            OrderIdSetter = (o, i) => propertyInfo?.SetValue(o, i);
        }

        [OneTimeSetUp]
        public static void SetupFixture()
        {
            // Ensure the log handler is still attached.
            Log.LogHandler = FixtureLogHandler;
            _underTest = new TerminalLinkBrokerage(MockOrderProvider.Object, ApiType.Desktop, Environment.Beta, Config.GetValue<string>("terminal-link-server-host"),
                Config.GetValue<int>("terminal-link-server-port"), MockTerminalLinkSymbolMapper.Object, new AggregationManager());
            _underTest.Connect();
        }

        [SetUp]
        public void SetUp()
        {
            // A log handler exists for single tests so that their output will be logged into the test unit runner for that test.
            _singleTestLogHandler = new ConsoleLogHandler();
            Log.LogHandler = _singleTestLogHandler;
        }

        [TearDown]
        public void TearDown()
        {
            _singleTestLogHandler?.Dispose();
        }

        [OneTimeTearDown]
        public static void PackUpFixture()
        {
            Log.LogHandler = FixtureLogHandler;
            _underTest?.Disconnect();
            _underTest?.Dispose();
            FixtureLogHandler?.Dispose();
        }

        [Test]
        [TestCaseSource(nameof(GetSymbols))]
        public void Can_Lookup_Futures(Symbol symbol)
        {
            Log.LogHandler = new ConsoleLogHandler();
            var result = _underTest.LookupSymbols(symbol, false).ToList();
            Assert.IsNotEmpty(result);
        }

        [Test]
        [TestCase("NGA COMB Comdty", OrderType.Limit, 1, 2)]
        [TestCase("ECH0 Curncy", OrderType.Market, 1, 2)]
        [TestCase("NGH0 COMB Comdty", OrderType.Limit, 2, 3, 4)]
        [TestCase("SSW SJ Equity", OrderType.Limit, 2, 3, 4)]
        [TestCase("BHP US Equity", OrderType.Limit, 5, 6, 7, 8)]
        [TestCase("1337 HK Equity", OrderType.Limit, 5, 6, 7, 8)]
        [TestCase("1337 HK Equity", OrderType.Limit, 5)]
        [TestCase("1337 HK Equity", OrderType.Limit, 8, 7, 6, 5)]
        [TestCase("BOA COMB Comdty", OrderType.Limit, 1, 2)]
        [TestCase("BOH0 COMB Comdty", OrderType.Limit, 2, 1)]
        [TestCase("BOA COMB Comdty", OrderType.Limit, 2, 3, 1)]
        public async Task Can_Manipulate_Order(string bbgSymbol, OrderType orderType, int initialQuantity, params int[] updatedQuantities)
        {
            // Setup & map a TerminalLink symbol
            MockTerminalLinkSymbolMapper.Setup(x => x.GetBrokerageSymbol(TestSymbol)).Returns(bbgSymbol);
            MockTerminalLinkSymbolMapper.Setup(x => x.GetLeanSymbol(bbgSymbol, null)).Returns(TestSymbol);

            // Setup
            var order = Order.CreateOrder(new SubmitOrderRequest(orderType, SecurityType.Future, TestSymbol, initialQuantity, 1, 1, DateTime.Now, null,
                new OrderProperties {TimeInForce = TimeInForce.Day}));
            OrderIdSetter(order, new Random().Next(0, int.MaxValue));
            MockOrderProvider.Setup(x => x.GetOrderById(order.Id)).Returns(order);

            // Place the order
            var result = await Run(_underTest.PlaceOrder, order);
            Assert.That(result.Message, Contains.Substring(TerminalLinkNames.CreateOrderAndRouteEx.ToString()));

            // Update the order
            foreach (var updateQuantity in updatedQuantities)
            {
                order.ApplyUpdateOrderRequest(new UpdateOrderRequest(DateTime.Now, order.Id, new UpdateOrderFields {Quantity = updateQuantity}));
                result = await Run(_underTest.UpdateOrder, order);
                Assert.That(result.Message, Contains.Substring(TerminalLinkNames.ModifyRouteEx.ToString()));
            }

            // Cancel / delete
            result = await Run(_underTest.CancelOrder, order);
            Assert.That(result.Message, Contains.Substring(TerminalLinkNames.CancelOrderEx.ToString()));
        }

        [Test]
        // Tick-level data for tick types
        [TestCase("BHP AU Equity", 1, Resolution.Tick, TickType.Quote)]
        [TestCase("BHP AU Equity", 1, Resolution.Tick, TickType.Trade)]
        // Multi-day
        [TestCase("ADH0 Curncy", OneDay * 2, Resolution.Daily, TickType.Trade)]
        [TestCase("ADH0 Curncy", OneDay * 2, Resolution.Daily, TickType.Quote)]
        // Minute-level resolution
        [TestCase("ADH0 Curncy", OneDay, Resolution.Minute, TickType.Trade)]
        [TestCase("ADH0 Curncy", OneDay, Resolution.Minute, TickType.Quote)]
        // Long
        [TestCase("ZGF0 COMB Curncy", OneDay * 32, Resolution.Minute, TickType.Quote)]
        [TestCase("ZGG0 COMB Curncy", OneDay * 32, Resolution.Minute, TickType.Quote)]
        // Extra asset classes
        [TestCase("BHP AU Equity", OneDay, Resolution.Daily, TickType.Trade)]
        [TestCase("AAPL US Equity", OneDay, Resolution.Daily, TickType.Trade)]
        // First month
        [TestCase("BO1 COMB Comdty", OneDay * 16, Resolution.Minute, TickType.Quote)]
        [TestCase("BO1 COMB Comdty", OneDay * 16, Resolution.Daily, TickType.Quote)]
        [TestCase("BO1 COMB Comdty", OneDay * 16, Resolution.Hour, TickType.Trade)]
        // Active month
        [TestCase("BO1 COMB Comdty", OneDay * 16, Resolution.Minute, TickType.Quote, Ignore = "")]
        [TestCase("BOA COMB Comdty", OneDay * 16, Resolution.Minute, TickType.Quote, Ignore = "")]
        [TestCase("BO1 COMB Comdty", OneDay * 16, Resolution.Daily, TickType.Quote, Ignore = "")]
        [TestCase("BOA COMB Comdty", OneDay * 16, Resolution.Daily, TickType.Quote, Ignore = "")]
        public void Can_Request_History(string bbSymbol, int minutes, Resolution resolution, TickType tickType)
        {
            Log.Trace("Receiving data for: " + bbSymbol);
            MockTerminalLinkSymbolMapper.Setup(x => x.GetLeanSymbol(bbSymbol, null)).Returns(TestSymbol);
            MockTerminalLinkSymbolMapper.Setup(x => x.GetBrokerageSymbol(TestSymbol)).Returns(bbSymbol);
            // Always rewind 1 day, so we guarantee market will be open.
            var endDate = new DateTime(2020, 02, 24, 01, 00, 00, DateTimeKind.Utc);
            switch (endDate.DayOfWeek)
            {
                case DayOfWeek.Sunday:
                    endDate = endDate.AddDays(-2);
                    break;
                case DayOfWeek.Saturday:
                    endDate = endDate.AddDays(-1);
                    break;
            }

            var startDate = endDate.AddMinutes(-minutes);
            var stopwatch = Stopwatch.StartNew();
            var request = new HistoryRequest(startDate, endDate, null, TestSymbol, resolution, SecurityExchangeHours.AlwaysOpen(DateTimeZone.Utc), DateTimeZone.Utc, null, true,
                false, DataNormalizationMode.Raw, tickType);
            var history = _underTest.GetHistory(request).ToList();
            stopwatch.Stop();
            Assert.IsNotEmpty(history);
            Log.Trace("Results: " + history.Count);
            Log.Trace("   Time: " + stopwatch.Elapsed.ToString("c"));
            Log.Trace("Receiving data for: " + bbSymbol);
        }

        [Test]
        [TestCase("BHP AU Equity")]
        [TestCase("BOA COMB Comdty")]
        public void Can_Retrieve_Live_Data(string bbgSymbol)
        {
            MockTerminalLinkSymbolMapper.Setup(x => x.GetBrokerageSymbol(TestSymbol)).Returns(bbgSymbol);
            MockTerminalLinkSymbolMapper.Setup(x => x.GetLeanSymbol(bbgSymbol, null)).Returns(TestSymbol);
            var config = new SubscriptionDataConfig(typeof(TradeBar), TestSymbol, Resolution.Tick,
                TimeZones.NewYork, TimeZones.NewYork, false, true, false);

            var enumerator = _underTest.Subscribe(config, (sender, args) => { });
            var ticks = new List<Tick>();
            var count = 10;
            do
            {
                if (enumerator.Current != null)
                {
                    Log.Trace(enumerator.Current.ToString());
                    ticks.Add(enumerator.Current as Tick);
                }
                else
                {
                    Thread.Sleep(100);
                }
            } while (enumerator.MoveNext() && --count > 0);

            Assert.That(ticks, Is.Not.Empty.And.All.Matches<Tick>(p => Equals(p.Symbol, TestSymbol)));
        }

        private static IEnumerable<Symbol> GetSymbols()
        {
            return MockTerminalLinkSymbolMapper.Object.MappingInfo.Select(x => Symbol.Create(x.Value.Underlying, x.Value.SecurityType, x.Value.Market));
        }

        private static Task<BrokerageMessageEvent> OnBrokerMessages(ManualResetEventSlim latch)
        {
            BrokerageMessageEvent result = null;

            void OnMessage(object _, BrokerageMessageEvent evt)
            {
                result = evt;
            }

            _underTest.Message += OnMessage;
            latch.Wait();
            _underTest.Message -= OnMessage;
            if (result == null)
            {
                Assert.Fail("No response");
            }
            else if (result.Type == BrokerageMessageType.Error)
            {
                Assert.Fail("Request responded with an error #{0}: {1}", result.Code, result.Message);
            }

            return Task.FromResult(result);
        }

        private static Task<List<OrderEvent>> NextOrderEvent(Order order)
        {
            return OnEvent<TerminalLinkBrokerage, List<OrderEvent>>(_underTest, (b, e) => b.OrdersStatusChanged += e, (b, e) => b.OrdersStatusChanged -= e,
                e => e.Single().OrderId == order.Id);
        }

        private static async Task<BrokerageMessageEvent> Run(Func<Order, bool> testFunc, Order order)
        {
            BrokerageMessageEvent message;
            var orderTask = NextOrderEvent(order);
            using (var latch = new ManualResetEventSlim())
            {
                var brokerMessagesTask = Task.Run(() => OnBrokerMessages(latch));
                var result = testFunc(order);
                Thread.Sleep(1000);
                latch.Set();
                message = await brokerMessagesTask;
                Assert.True(result);
            }

            // Wait for the OnNewOrder,OnOrderUpdate event. etc.
            await orderTask;
            return message;
        }

        /// <summary>
        /// Runs a task that awaits the next call.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <typeparam name="TResult"></typeparam>
        /// <param name="item"></param>
        /// <param name="subscribe"></param>
        /// <param name="unsubscribe"></param>
        /// <param name="conditionalFunc"></param>
        /// <returns></returns>
        private static Task<TResult> OnEvent<T, TResult>(T item, Action<T, EventHandler<TResult>> subscribe, Action<T, EventHandler<TResult>> unsubscribe,
            Func<TResult, bool> conditionalFunc = null)
        {
            var taskCompletionSource = new TaskCompletionSource<TResult>();

            void Handler(object _, TResult e)
            {
                if (conditionalFunc == null || conditionalFunc(e)) taskCompletionSource.TrySetResult(e);
            }

            subscribe(item, Handler);
            return Task.Run(() => taskCompletionSource.Task)
                .ContinueWith(t =>
                {
                    unsubscribe(item, Handler);
                    return t.Result;
                });
        }
    }
}