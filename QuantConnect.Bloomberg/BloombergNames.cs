﻿/*
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
        /// Value: EMSX_FILL_PRICE
        /// </summary>
        public static readonly Name EMSXFillPrice = new Name("EMSX_FILL_PRICE");
        /// <summary>
        /// Value: EMSX_FILL_AMOUNT
        /// </summary>
        public static readonly Name EMSXFillAmount = new Name("EMSX_FILL_AMOUNT");
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
        /// Value: ModifyOrderEx
        /// </summary>
        public static readonly Name ModifyOrderEx = new Name("ModifyOrderEx");
        /// <summary>
        /// Value: DeleteOrder
        /// </summary>
        public static readonly Name DeleteOrder = new Name("DeleteOrder");

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
