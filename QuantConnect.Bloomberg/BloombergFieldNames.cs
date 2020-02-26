namespace QuantConnect.Bloomberg
{
    /// <summary>
    ///     Names of Bloomberg's fields (FLDS).
    /// </summary>
    public static class BloombergFieldNames
    {
        public const string OpenInterest = "RT_OPEN_INTEREST";
        public const string LastPrice = "LAST_PRICE";
        public const string LastTradeSize = "SIZE_LAST_TRADE";
        public const string Bid = "BID";
        public const string BidSize = "BID_SIZE";
        public const string Ask = "ASK";
        public const string AskSize = "ASK_SIZE";
        public const string FuturesChain = "FUT_CHAIN";
    }
}