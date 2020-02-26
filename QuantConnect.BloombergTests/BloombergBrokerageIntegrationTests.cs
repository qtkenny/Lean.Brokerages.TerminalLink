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
using QuantConnect.Bloomberg;
using QuantConnect.Brokerages;
using QuantConnect.Configuration;
using QuantConnect.Data.Market;
using QuantConnect.Logging;
using QuantConnect.Orders;
using QuantConnect.Packets;
using QuantConnect.Securities;
using DateTime = System.DateTime;
using Environment = QuantConnect.Bloomberg.Environment;
using HistoryRequest = QuantConnect.Data.HistoryRequest;
using LimitOrder = QuantConnect.Orders.LimitOrder;
using Order = QuantConnect.Orders.Order;
using OrderType = QuantConnect.Orders.OrderType;
using TimeInForce = QuantConnect.Orders.TimeInForce;

namespace QuantConnect.BloombergTests
{
    [TestFixture]
    //[Ignore("Broker tests should be executed manually")]
    [Timeout(60000)]
    public class BloombergBrokerageIntegrationTests
    {
        private const int OneDay = 60 * 24;
        private static readonly ConsoleLogHandler FixtureLogHandler = new ConsoleLogHandler();
        private static readonly Action<Order, int> OrderIdSetter;
        private static readonly Symbol TestSymbol = Symbol.Empty;
        private static readonly Mock<IOrderProvider> MockOrderProvider = new Mock<IOrderProvider>();
        private static readonly Mock<BloombergSymbolMapper> MockBloombergSymbolMapper = new Mock<BloombergSymbolMapper>("integration-bloomberg-symbol-map.json") {CallBase = true};
        private static BloombergBrokerage _underTest;
        private ConsoleLogHandler _singleTestLogHandler;

        static BloombergBrokerageIntegrationTests()
        {
            Log.LogHandler = FixtureLogHandler;
            Config.SetConfigurationFile("integration-config.json");
            Config.Reset();

            // Work out the order
            var propertyInfo = typeof(LimitOrder).GetProperty(nameof(Order.Id), BindingFlags.Public | BindingFlags.Instance);
            OrderIdSetter = (o, i) => propertyInfo?.SetValue(o, i);
        }

        [TestFixtureSetUp]
        public static void SetupFixture()
        {
            // Ensure the log handler is still attached.
            Log.LogHandler = FixtureLogHandler;
            _underTest = new BloombergBrokerage(MockOrderProvider.Object, ApiType.Desktop, Environment.Beta, MockBloombergSymbolMapper.Object,
                Config.GetValue<string>("bloomberg-server-host"), Config.GetValue<int>("bloomberg-server-port"));
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

        [TestFixtureTearDown]
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
            var result = _underTest.LookupSymbols(symbol.Value, symbol.SecurityType).ToList();
            Assert.IsNotEmpty(result);
        }

        [Test]
        [TestCase("NGA COMB Comdty", false, 1, 2)]
        [TestCase("NGH0 COMB Comdty", true, 2, 3, 4)]
        [TestCase("SSW SJ Equity", true, 2, 3, 4)]
        [TestCase("BHP AU Equity", true, 5, 4, 3, 2)]
        [TestCase("BOA COMB Comdty", true, 1, 2)]
        [TestCase("BOH0 COMB Comdty", true, 2, 1)]
        [TestCase("BOA COMB Comdty", true, 2, 3, 1)]
        public async Task Can_Manipulate_Order(string bbgSymbol, bool delete, int initialQuantity, params int[] updatedQuantities)
        {
            // Setup & map a Bloomberg symbol
            MockBloombergSymbolMapper.Setup(x => x.GetBrokerageSymbol(TestSymbol)).Returns(bbgSymbol);
            MockBloombergSymbolMapper.Setup(x => x.GetLeanSymbol(bbgSymbol, null)).Returns(TestSymbol);

            // Setup
            var order = Order.CreateOrder(new SubmitOrderRequest(OrderType.Limit, SecurityType.Future, TestSymbol, initialQuantity, 1, 1, DateTime.Now, null,
                new OrderProperties {TimeInForce = TimeInForce.Day}));
            var orderId = new Random().Next(0, int.MaxValue);
            OrderIdSetter(order, orderId);
            MockOrderProvider.Setup(x => x.GetOrderById(order.Id)).Returns(order);

            // Place the order
            var bbOrderTask = OnBloombergOrder();
            var brokerMessage = await OnNextMessage(_underTest.PlaceOrder, order, bbOrderTask);
            Assert.That(brokerMessage.Message, Contains.Substring(BloombergNames.CreateOrderAndRouteEx.ToString()));
            var bbOrder = bbOrderTask.Result;
            var fieldAmount = bbOrder.GetField("EMSX_AMOUNT");
            Assert.AreEqual(initialQuantity, int.Parse(fieldAmount.CurrentValue));

            // Update the order
            foreach (var updateQuantity in updatedQuantities)
            {
                order.ApplyUpdateOrderRequest(new UpdateOrderRequest(DateTime.Now, order.Id, new UpdateOrderFields {Quantity = updateQuantity}));
                brokerMessage = await OnNextMessage(_underTest.UpdateOrder, order, AwaitFieldUpdate(fieldAmount, updateQuantity.ToString()));
                Assert.That(brokerMessage.Message, Contains.Substring(BloombergNames.ModifyOrderEx.ToString()));
            }

            // Cancel / delete
            if (delete)
            {
                brokerMessage = await OnNextMessage(_underTest.CancelOrder, order);
                Assert.That(brokerMessage.Message, Contains.Substring(BloombergNames.CancelOrderEx.ToString()));
            }
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
        [TestCase("BO1 COMB Comdty", OneDay * 16, Resolution.Minute, TickType.Quote, Ignore = true)]
        [TestCase("BOA COMB Comdty", OneDay * 16, Resolution.Minute, TickType.Quote, Ignore = true)]
        [TestCase("BO1 COMB Comdty", OneDay * 16, Resolution.Daily, TickType.Quote, Ignore = true)]
        [TestCase("BOA COMB Comdty", OneDay * 16, Resolution.Daily, TickType.Quote, Ignore = true)]
        public void Can_Request_History(string bbSymbol, int minutes, Resolution resolution, TickType tickType)
        {
            Log.Trace("Receiving data for: " + bbSymbol);
            MockBloombergSymbolMapper.Setup(x => x.GetLeanSymbol(bbSymbol, null)).Returns(TestSymbol);
            MockBloombergSymbolMapper.Setup(x => x.GetBrokerageSymbol(TestSymbol)).Returns(bbSymbol);
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
            MockBloombergSymbolMapper.Setup(x => x.GetBrokerageSymbol(TestSymbol)).Returns(bbgSymbol);
            MockBloombergSymbolMapper.Setup(x => x.GetLeanSymbol(bbgSymbol, null)).Returns(TestSymbol);
            _underTest.Subscribe(new LiveNodePacket(), new[] {TestSymbol});
            Tick[] ticks;
            while (true)
            {
                ticks = (Tick[]) _underTest.GetNextTicks();
                if (ticks != null && ticks.Length > 0)
                {
                    break;
                }

                Thread.Sleep(0);
            }

            Assert.That(ticks, Is.Not.Empty.And.All.Matches<Tick>(p => Equals(p.Symbol, TestSymbol)));
        }

        private static IEnumerable<Symbol> GetSymbols()
        {
            return MockBloombergSymbolMapper.Object.MappingInfo.Select(x => Symbol.Create(x.Value.Underlying, x.Value.SecurityType, x.Value.Market));
        }

        private static async Task<BrokerageMessageEvent> OnNextMessage<T, TResult>(Func<T, TResult> function, T order, params Task[] additionalTasksToAwait)
        {
            var brokerMessageTask = OnEvent<BloombergBrokerage, BrokerageMessageEvent>(_underTest, (b, e) => b.Message += e, (b, e) => b.Message -= e);
            function(order);
            var result = await brokerMessageTask;
            if (result.Type == BrokerageMessageType.Error)
            {
                Assert.Fail("Request responded with an error #{0}: {1}", result.Code, result.Message);
            }
            if (additionalTasksToAwait != null && additionalTasksToAwait.Length > 0)
            {
                await Task.WhenAll(additionalTasksToAwait);
            }

            return result;
        }

        private static Task<BloombergOrder> OnBloombergOrder()
        {
            return OnEvent<BloombergOrders, BloombergOrder>(_underTest.Orders, (o, h) => o.OrderCreated += h, (o, h) => o.OrderCreated -= h);
        }

        private static Task AwaitFieldUpdate(BloombergField bbOrder, string expectedValue)
        {
            return OnEvent<BloombergField, EventArgs>(bbOrder, (o, h) => o.Updated += h, (o, h) => o.Updated -= h, r => bbOrder.CurrentValue == expectedValue);
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