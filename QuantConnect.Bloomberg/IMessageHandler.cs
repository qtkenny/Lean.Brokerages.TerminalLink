/*
* QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
* Lean Algorithmic Trading Engine v2.2 Copyright 2015 QuantConnect Corporation.
*/

using Bloomberglp.Blpapi;

namespace QuantConnect.Bloomberg
{
    public interface IMessageHandler
    {
        void ProcessMessage(Message message);
    }
}
