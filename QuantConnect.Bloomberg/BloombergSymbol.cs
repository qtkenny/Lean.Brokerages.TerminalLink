using Newtonsoft.Json;

namespace QuantConnect.Bloomberg
{
    /// <summary>
    ///     Breakdown of a Bloomberg symbol to the Lean equivalent.
    /// </summary>
    /// <see cref="QuantConnect.Symbol" />
    public class BloombergSymbol
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

        #region Futures

        /// <summary>
        /// Expiry details of the future (optional).
        /// </summary>
        public string ExpiryMonthYear { get; set; }

        #endregion
    }
}