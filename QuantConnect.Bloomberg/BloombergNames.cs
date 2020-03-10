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
        /// Value: EMSX_BROKER
        /// </summary>
        public static readonly Name EMSXBroker = new Name("EMSX_BROKER");

        /// <summary>
        /// Value: EMSX_HAND_INSTRUCTION
        /// </summary>
        public static readonly Name EMSXHandInstruction = new Name("EMSX_HAND_INSTRUCTION");

        /// <summary>
        /// Value: EMSX_ACCOUNT
        /// </summary>
        public static readonly Name EMSXAccount = new Name("EMSX_ACCOUNT");

        /// <summary>
        /// Value: EMSX_NOTES
        /// </summary>
        public static readonly Name EMSXNotes = new Name("EMSX_NOTES");

        /// <summary>
        /// Value: EMSX_TICKER
        /// </summary>
        public static readonly Name EMSXTicker = new Name("EMSX_TICKER");

        /// <summary>
        /// Value: EMSX_ASSET_CLASS
        /// </summary>
        public static readonly Name EMSXAssetClass = new Name("EMSX_ASSET_CLASS");

        /// <summary>
        /// Value: EMSX_ORDER_TYPE
        /// </summary>
        public static readonly Name EMSXOrderType = new Name("EMSX_ORDER_TYPE");

        /// <summary>
        /// Value: EMSX_SIDE
        /// </summary>
        public static readonly Name EMSXSide = new Name("EMSX_SIDE");

        /// <summary>
        /// Value: EMSX_TIF
        /// </summary>
        public static readonly Name EMSXTif = new Name("EMSX_TIF");

        /// <summary>
        /// Value: EMSX_GTD_DATE
        /// </summary>
        public static readonly Name EMSXGTDDate = new Name("EMSX_GTD_DATE");

        /// <summary>
        /// Value: EMSX_STATUS
        /// </summary>
        public static readonly Name EMSXStatus = new Name("EMSX_STATUS");

        /// <summary>
        /// Value: EMSX_AMOUNT
        /// </summary>
        public static readonly Name EMSXAmount = new Name("EMSX_AMOUNT");

        /// <summary>
        /// Value: EMSX_LIMIT_PRICE
        /// </summary>
        public static readonly Name EMSXLimitPrice = new Name("EMSX_LIMIT_PRICE");

        /// <summary>
        /// Value: EMSX_STOP_PRICE
        /// </summary>
        public static readonly Name EMSXStopPrice = new Name("EMSX_STOP_PRICE");

        /// <summary>
        /// Value: EMSX_DATE
        /// </summary>
        public static readonly Name EMSXDate = new Name("EMSX_DATE");

        /// <summary>
        /// Value: EMSX_TIME_STAMP_MICROSEC
        /// </summary>
        public static readonly Name EMSXTimeStampMicrosec = new Name("EMSX_TIME_STAMP_MICROSEC");

        /// <summary>
        /// Value: EMSX_AVG_PRICE
        /// </summary>
        public static readonly Name EMSXAvgPrice = new Name("EMSX_AVG_PRICE");

        /// <summary>
        /// Value: EMSX_FILLED
        /// </summary>
        public static readonly Name EMSXFilled = new Name("EMSX_FILLED");

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
        public static readonly Name EMSXReferenceRouteId = new Name("EMSX_ROUTE_REF_ID");

        #endregion EMSX

        /// <summary>
        /// Value: MESSAGE
        /// </summary>
        public static readonly Name Message = new Name("MESSAGE");
        /// <summary>
        /// Value: MSG_SUB_TYPE
        /// </summary>
        public static readonly Name MessageSubType = new Name("MSG_SUB_TYPE");

        /// <summary>
        /// Value: STATUS
        /// </summary>
        public static readonly Name Status = new Name("STATUS");

        /// <summary>
        /// Value: EVENT_STATUS
        /// </summary>
        public static readonly Name EventStatus = new Name("EVENT_STATUS");

        /// <summary>
        /// Value: MKTDATA_EVENT_TYPE
        /// </summary>
        public static readonly Name MktdataEventType = new Name("MKTDATA_EVENT_TYPE");

        /// <summary>
        /// Value: MKTDATA_EVENT_SUBTYPE
        /// </summary>
        public static readonly Name MktdataEventSubtype = new Name("MKTDATA_EVENT_SUBTYPE");

        /// <summary>
        /// Value: BID_UPDATE_STAMP_RT
        /// </summary>
        public static readonly Name BidUpdateStamp = new Name("BID_UPDATE_STAMP_RT");

        /// <summary>
        /// Value: ASK_UPDATE_STAMP_RT
        /// </summary>
        public static readonly Name AskUpdateStamp = new Name("ASK_UPDATE_STAMP_RT");

        /// <summary>
        /// Value: TRADE_UPDATE_STAMP_RT
        /// </summary>
        public static readonly Name TradeUpdateStamp = new Name("TRADE_UPDATE_STAMP_RT");

        /// <summary>
        /// Value: BLOOMBERG_SEND_TIME_RT
        /// </summary>
        public static readonly Name BloombergSendTime = new Name("BLOOMBERG_SEND_TIME_RT");

        /// <summary>
        /// Value: CreateOrderAndRouteEx.  'Ex' is the extended method.
        /// </summary>
        public static readonly Name CreateOrderAndRouteEx = new Name("CreateOrderAndRouteEx");

        /// <summary>
        /// Value: ModifyOrderEx
        /// </summary>
        public static readonly Name ModifyOrderEx = new Name("ModifyOrderEx");

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

        // RESPONSE VALUES
        public static readonly Name ErrorInfo = new Name("ErrorInfo");
        /// <summary>
        /// Value: ERROR_CODE
        /// </summary>
        public static readonly Name ErrorCode = new Name("ERROR_CODE");
        /// <summary>
        /// Value: ERROR_MESSAGE
        /// </summary>
        public static readonly Name ErrorMessage = new Name("ERROR_MESSAGE");
        public static readonly Name SecurityData = new Name("securityData");
        public static readonly Name FieldData = new Name("fieldData");
        public static readonly Name Date = new Name("date");
        public static readonly Name ResponseError = new Name("responseError");

        // REF DATA REQUEST
        public static readonly Name Fields = new Name("fields");
        public static readonly Name Securities = new Name("securities");

        /// <summary>
        /// Value: OPEN
        /// </summary>
        public static readonly Name OpenHistorical = new Name("OPEN");
        /// <summary>
        /// Value: HIGH
        /// </summary>
        public static readonly Name HighHistorical = new Name("HIGH");
        /// <summary>
        /// Value: LOW
        /// </summary>
        public static readonly Name LowHistorical = new Name("LOW");
        /// <summary>
        /// Value: BLOOMBERG_CLOSE_PRICE
        /// </summary>
        public static readonly Name BloombergClosePrice = new Name("BLOOMBERG_CLOSE_PRICE");
        /// <summary>
        /// Value: PX_LAST
        /// </summary>
        public static readonly Name PxLast = new Name("PX_LAST");
        public static readonly Name Volume = new Name("VOLUME");

        /// <summary>
        /// Value: open
        /// </summary>
        public static readonly Name OpenIntraday = new Name("open");
        /// <summary>
        /// Value: high
        /// </summary>
        public static readonly Name HighIntraday = new Name("high");
        /// <summary>
        /// Value: low
        /// </summary>
        public static readonly Name LowIntraday = new Name("low");
        /// <summary>
        /// Value: close
        /// </summary>
        public static readonly Name CloseIntraday = new Name("close");
        public static readonly Name BarData = new Name("barData");
        public static readonly Name BarTickData = new Name("barTickData");
        public static readonly Name TickData = new Name("tickData");
        public static readonly Name Time = new Name("time");
        public static readonly Name BestBid = new Name("BID_BEST");
        public static readonly Name BestAsk = new Name("ASK_BEST");
        public static readonly Name Security = new Name("security");
        public static readonly Name EventType = new Name("eventType");
        public static readonly Name EventTypes = new Name("eventTypes");
        public static readonly Name StartDateTime = new Name("startDateTime");
        public static readonly Name EndDateTime = new Name("endDateTime");
        public static readonly Name Size = new Name("size");
        public static readonly Name Value = new Name("value");
        public static readonly Name Type = new Name("type");

        // MKTDATA_EVENT_TYPE
        public static readonly Name Trade = new Name("TRADE");
        public static readonly Name Quote = new Name("QUOTE");
        public static readonly Name Summary = new Name("SUMMARY");

        // MKTDATA_EVENT_SUBTYPE
        public static readonly Name New = new Name("NEW");
        public static readonly Name Bid = new Name("BID");
        public static readonly Name Ask = new Name("ASK");
        public static readonly Name InitPaint = new Name("INITPAINT");
        public static readonly Name Intraday = new Name("INTRADAY");
    }
}
