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

        #region EMSX
        /// <summary>
        /// Value: EMSX_SEQUENCE
        /// </summary>
        public static readonly Name EMSXSequence = new Name("EMSX_SEQUENCE");
        /// <summary>
        /// Value: EMSX_ROUTE_ID
        /// </summary>
        public static readonly Name EMSXRouteId = new Name("EMSX_ROUTE_ID");
        /// <summary>
        /// Value: EMSX_STATUS
        /// </summary>
        public static readonly Name EMSXStatus = new Name("EMSX_STATUS");
        /// <summary>
        /// Value: EMSX_AVG_PRICE
        /// </summary>
        public static readonly Name EMSXAvgPrice = new Name("EMSX_AVG_PRICE");
        /// <summary>
        /// Value: EMSX_FILLED
        /// </summary>
        public static readonly Name EMSXFilled = new Name("EMSX_FILLED");
        /// <summary>
        /// Value: ErrorInfo
        /// </summary>
        public static readonly Name OrderErrorInfo = new Name("ErrorInfo");
        /// <summary>
        /// Value: EMSX_ORDER_REF_ID
        /// </summary>
        public static readonly Name EMSXReferenceOrderIdRequest = new Name("EMSX_ORDER_REF_ID");
        /// <summary>
        /// Value: EMSX_ORD_REF_ID
        /// </summary>
        public static readonly Name EMSXReferenceOrderIdResponse = new Name("EMSX_ORD_REF_ID");
        /// <summary>
        /// Value: EMSX_ROUTE_REF_ID
        /// </summary>
        public static readonly Name EMSXRouteRefId = new Name("EMSX_ROUTE_REF_ID");
        #endregion EMSX

        public static readonly Name EventStatus = new Name("EVENT_STATUS");
        /// <summary>
        /// Value: MSG_SUB_TYPE
        /// </summary>
        public static readonly Name MessageSubType = new Name("MSG_SUB_TYPE");
        /// <summary>
        /// Value: CreateOrderAndRouteEx.  'Ex' is the extended method.
        /// </summary>
        public static readonly Name CreateOrderAndRouteEx = new Name("CreateOrderAndRouteEx");
        /// <summary>
        /// Value: ModifyRouteEx
        /// </summary>
        public static readonly Name ModifyRouteEx = new Name("ModifyRouteEx");
        /// <summary>
        /// Value: CancelOrderEx
        /// </summary>
        public static readonly Name CancelOrderEx = new Name("CancelOrderEx");

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
