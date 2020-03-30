using Newtonsoft.Json;

namespace QuantConnect.Bloomberg
{
    /// <summary>
    ///     Breakdown of a Bloomberg symbol to the Lean equivalent.
    /// </summary>
    /// <see cref="QuantConnect.Symbol" />
    public class BloombergMappingInfo
    {
        /// <summary>
        ///     Equivalent to <see cref="SecurityIdentifier.Symbol" />
        /// </summary>
        [JsonRequired]
        public string Underlying { get; set; }

        /// <summary>
        ///     Equivalent to <see cref="SecurityIdentifier.SecurityType" />.
        /// </summary>
        [JsonRequired]
        public SecurityType SecurityType { get; set; }

        /// <summary>
        ///     Equivalent to <see cref="SecurityIdentifier.Market" />.
        /// </summary>
        [JsonRequired]
        public string Market { get; set; }

        /// <summary>
        ///     Equivalent to specifying <see cref="Symbol.Value" />.
        /// </summary>
        public string Alias { get; set; }

        #region Futures & Options

        /// <summary>
        ///     (Optional) Allows the exact Bloomberg futures / options chain to be specified.
        ///     This will be used over querying Bloomberg itself for the contents of the chain.
        /// </summary>
        public string[] Chain { get; set; }

        /// <summary>
        /// The suffix used to obtain the BBG ticker (e.g. "COMB Comdty")
        /// </summary>
        public string TickerSuffix { get; set; }

        /// <summary>
        /// The suffix appended to the BBG root symbol when requesting the full chain (e.g. "1")
        /// </summary>
        public string RootLookupSuffix { get; set; }

        #endregion
    }
}