using System.Runtime.Serialization;
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
        ///     Expiry details of the future.
        /// </summary>
        public string ExpiryMonthYear { get; set; }

        #endregion

        /// <summary>
        ///     Will be called after <see cref="BloombergSymbol" /> has been deserialized.
        /// </summary>
        /// <param name="_"></param>
        [OnDeserialized]
        public void OnDeserialized(StreamingContext _)
        {
            switch (SecurityType)
            {
                // Validate futures
                case SecurityType.Future:
                    if (string.IsNullOrWhiteSpace(ExpiryMonthYear))
                    {
                        throw new SerializationException($"{nameof(ExpiryMonthYear)} must not be blank for futures [underlying:{Underlying}]");
                    }

                    break;
            }
        }
    }
}