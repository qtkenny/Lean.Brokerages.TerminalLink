using System;
using System.Reflection;
using System.Threading.Tasks;
using Moq;
using NUnit.Framework;
using QuantConnect.Bloomberg;
using QuantConnect.Brokerages;
using QuantConnect.Configuration;
using QuantConnect.Logging;
using QuantConnect.Orders;
using QuantConnect.Securities;
using QuantConnect.Securities.Future;
using Environment = QuantConnect.Bloomberg.Environment;

namespace QuantConnect.BloombergTests
{
    [TestFixture]
    //[Ignore("Broker tests should be executed manually")]
    [Timeout(10000)]
    public class BloombergBrokerageIntegrationTests
    {
        private static readonly Action<Order, int> OrderIdSetter;
        private static readonly ConsoleLogHandler ConsoleLogHandler = new ConsoleLogHandler();
        private static readonly Symbol TestSymbol;
        private static readonly Mock<IOrderProvider> MockOrderProvider = new Mock<IOrderProvider>();
        private static readonly Mock<IBloombergSymbolMapper> MockBloombergSymbolMapper = new Mock<IBloombergSymbolMapper>();
        private static BloombergBrokerage _underTest;

        static BloombergBrokerageIntegrationTests()
        {
            Log.LogHandler = ConsoleLogHandler;
            Config.SetConfigurationFile("integration-config.json");
            Config.Reset();

            // Work out the order
            var propertyInfo = typeof(LimitOrder).GetProperty(nameof(Order.Id), BindingFlags.Public | BindingFlags.Instance);
            OrderIdSetter = (o, i) => propertyInfo?.SetValue(o, i);

            // Setup the lean symbol
            var symbolBase = Config.GetValue<string>("symbol-base");
            var expiryFunction = FuturesExpiryFunctions.FuturesExpiryFunction(symbolBase);
            var nextMonth = DateTime.Now.AddMonths(1);
            nextMonth = new DateTime(nextMonth.Year, nextMonth.Month, 01);
            var expiry = expiryFunction.Invoke(nextMonth);
            TestSymbol = Symbol.CreateFuture(symbolBase, Config.GetValue<string>("market"), expiry);

            // Setup & map a Bloomberg symbol
            var bbgSymbol = Config.GetValue<string>("bloomberg-symbol");
            MockBloombergSymbolMapper.Setup(x => x.GetBrokerageSymbol(TestSymbol)).Returns(bbgSymbol);
            MockBloombergSymbolMapper.Setup(x => x.GetLeanSymbol(bbgSymbol)).Returns(TestSymbol);
        }

        [TestFixtureSetUp]
        public static void SetupFixture()
        {
            _underTest = new BloombergBrokerage(MockOrderProvider.Object, ApiType.Desktop, Environment.Beta, MockBloombergSymbolMapper.Object,
                Config.GetValue<string>("bloomberg-server-host"), Config.GetValue<int>("bloomberg-server-port"));
            _underTest.Connect();
        }

        [TestFixtureTearDown]
        public static void PackUpFixture()
        {
            _underTest?.Disconnect();
            _underTest?.Dispose();
            ConsoleLogHandler?.Dispose();
        }

        [Test]
        [TestCase(1, 2)]
        [TestCase(2, 1)]
        [TestCase(2, 1, 3)]
        [TestCase(2, 3, 1)]
        public async Task Can_Manipulate_Order(int initialQuantity, params int[] updatedQuantities)
        {
            // Setup
            var order = Order.CreateOrder(new SubmitOrderRequest(Config.GetValue<OrderType>("order-type"), Config.GetValue<SecurityType>("security-type"), TestSymbol,
                initialQuantity, 1, 100, DateTime.Now, null, new OrderProperties {TimeInForce = TimeInForce.Day}));
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
                brokerMessage = await OnNextMessage(_underTest.UpdateOrder, order, AwaitFieldUpdate(fieldAmount));
                Assert.That(brokerMessage.Message, Contains.Substring(BloombergNames.ModifyOrderEx.ToString()));
                Assert.AreEqual(updateQuantity, int.Parse(fieldAmount.CurrentValue));
            }

            // Cancel / delete
            var fieldStatus = bbOrder.GetField("EMSX_STATUS");
            brokerMessage = await OnNextMessage(_underTest.CancelOrder, order, AwaitFieldUpdate(fieldStatus));
            Assert.That(brokerMessage.Message, Contains.Substring(BloombergNames.DeleteOrder.ToString()));
            Assert.AreEqual("CANCEL", fieldStatus.CurrentValue);
        }

        private static async Task<BrokerageMessageEvent> OnNextMessage(Func<Order, bool> function, Order order, params Task[] additionalTasksToAwait)
        {
            var brokerMessageTask = OnEvent<BloombergBrokerage, BrokerageMessageEvent>(_underTest, (b, e) => b.Message += e, (b, e) => b.Message -= e);
            function(order);
            var result = await brokerMessageTask;
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

        private static Task AwaitFieldUpdate(BloombergField bbOrder)
        {
            return OnEvent<BloombergField, EventArgs>(bbOrder, (o, h) => o.Updated += h, (o, h) => o.Updated -= h);
        }

        /// <summary>
        /// Runs a task that awaits the next call.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <typeparam name="TResult"></typeparam>
        /// <param name="item"></param>
        /// <param name="subscribe"></param>
        /// <param name="unsubscribe"></param>
        /// <returns></returns>
        private static Task<TResult> OnEvent<T, TResult>(T item, Action<T, EventHandler<TResult>> subscribe, Action<T, EventHandler<TResult>> unsubscribe)
        {
            var taskCompletionSource = new TaskCompletionSource<TResult>();

            void Handler(object _, TResult e)
            {
                taskCompletionSource.TrySetResult(e);
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