namespace QuantConnect.TerminalLink
{
    /// <summary>
    ///     Names of TerminalLink's fields (FLDS).
    /// </summary>
    public static class TerminalLinkFieldNames
    {
        public const string OpenInterest = "RT_OPEN_INTEREST";
        public const string LastPrice = "LAST_PRICE";
        public const string LastTradeSize = "SIZE_LAST_TRADE";
        public const string Bid = "BID";
        public const string BidSize = "BID_SIZE";
        public const string Ask = "ASK";
        public const string AskSize = "ASK_SIZE";
        public const string FuturesChain = "FUT_CHAIN";
        public const string OptionsChain = "OPT_CHAIN";

        // TRADE EVENTS
        public const string TradePrice = "EVT_TRADE_PRICE_RT";
        public const string TradeSize = "EVT_TRADE_SIZE_RT";
        //public const string TradeTime = "EVT_TRADE_TIME_RT";
        public const string TradeDate = "EVT_TRADE_DATE_RT";
    }
}