/*
* QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
* Lean Algorithmic Trading Engine v2.2 Copyright 2015 QuantConnect Corporation.
*/

using QuantConnect.Logging;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace QuantConnect.Bloomberg
{
    public class BloombergOrders : IEnumerable<BloombergOrder>
    {
        private readonly List<BloombergOrder> _orders = new List<BloombergOrder>();
        private readonly BloombergBrokerage _brokerage;

        public BloombergOrders(BloombergBrokerage brokerage)
        {
            _brokerage = brokerage;
        }

        public void Subscribe()
        {
            Log.Trace("Orders: Subscribing");

            var orderTopic = _brokerage.GetServiceName(ServiceType.Ems) + "/order";

            //if (emsxapi.team != null) orderTopic = orderTopic + ";team=" + emsxapi.team.name;

            orderTopic = orderTopic + "?fields=";

            foreach (var f in _brokerage.OrderFields)
            {
                if (f.Name.Equals("EMSX_ORDER_REF_ID"))
                {
                    // Workaround for schema field naming
                    orderTopic = orderTopic + "EMSX_ORD_REF_ID" + ",";
                }
                else
                {
                    orderTopic = orderTopic + f.Name + ",";
                }
            }


            orderTopic = orderTopic.Substring(0,orderTopic.Length-1); // remove extra comma character

            _brokerage.Subscribe(orderTopic, new OrderSubscriptionHandler(this));

            //Log.Trace("Entering Order subscription lock");
            //while(!emsxapi.orderBlotterInitialized){
            //    Thread.Sleep(1);
            //}
            //Log.Trace("Order subscription lock released");
        }

        public BloombergOrder CreateOrder(int sequence)
        {
            var order = new BloombergOrder(sequence);

            _orders.Add(order);

            return order;
        }

        public BloombergOrder GetBySequenceNumber(int sequence)
        {
            return _orders.FirstOrDefault(o => o.Sequence == sequence);
        }

        public IEnumerator<BloombergOrder> GetEnumerator()
        {
            return _orders.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}
