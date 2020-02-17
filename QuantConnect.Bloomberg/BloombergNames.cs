/*
* QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
* Lean Algorithmic Trading Engine v2.2 Copyright 2015 QuantConnect Corporation.
*/

using Bloomberglp.Blpapi;

namespace QuantConnect.Bloomberg
{
    public static class BloombergNames
    {
        // EVENTS
        public static readonly Name OrderRouteFields = new Name("OrderRouteFields");

        // ADMIN
        public static readonly Name SlowConsumerWarning = new Name("SlowConsumerWarning");
        public static readonly Name SlowConsumerWarningCleared = new Name("SlowConsumerWarningCleared");

        // SESSION_STATUS
        public static readonly Name SessionStarted = new Name("SessionStarted");
        public static readonly Name SessionTerminated = new Name("SessionTerminated");
        public static readonly Name SessionStartupFailure = new Name("SessionStartupFailure");
        public static readonly Name SessionConnectionUp = new Name("SessionConnectionUp");
        public static readonly Name SessionConnectionDown = new Name("SessionConnectionDown");

        // SERVICE_STATUS
        public static readonly Name ServiceOpened = new Name("ServiceOpened");
        public static readonly Name ServiceOpenFailure = new Name("ServiceOpenFailure");

        // SUBSCRIPTION_STATUS + SUBSCRIPTION_DATA
        public static readonly Name SubscriptionFailure = new Name("SubscriptionFailure");
        public static readonly Name SubscriptionStarted = new Name("SubscriptionStarted");
        public static readonly Name SubscriptionStreamsActivated = new Name("SubscriptionStreamsActivated");
        public static readonly Name SubscriptionTerminated = new Name("SubscriptionTerminated");
    }
}
