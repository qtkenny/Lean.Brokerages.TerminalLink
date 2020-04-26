/*
* QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
* Lean Algorithmic Trading Engine v2.2 Copyright 2015 QuantConnect Corporation.
*/

using System.IO;
using Newtonsoft.Json;
using NUnit.Framework;
using QuantConnect.Bloomberg;

namespace QuantConnect.BloombergTests
{
    [TestFixture]
    public class BloombergMappingInfoTests
    {
        private static readonly JsonSerializer Serializer = new JsonSerializer();
        private readonly BloombergMappingInfo _underTest =
            new BloombergMappingInfo {Alias = "Alias1", Market = "Market1", Underlying = "Underlying1", SecurityType = SecurityType.Cfd, TickerSuffix = "1", RootLookupSuffix = "A"};

        [Test]
        public void Can_Serialize_And_Deserialize()
        {
            var json = Serialize(_underTest);
            Assert.NotNull(json);
            var actual = Deserialize(json);
            Assert.AreEqual(_underTest.Underlying, actual.Underlying);
            Assert.AreEqual(_underTest.Market, actual.Market);
            Assert.AreEqual(_underTest.SecurityType, actual.SecurityType);
            Assert.AreEqual(_underTest.Alias, actual.Alias);
            Assert.AreEqual(_underTest.RootLookupSuffix, actual.RootLookupSuffix);
            Assert.AreEqual(_underTest.TickerSuffix, actual.TickerSuffix);
        }

        private static string Serialize(BloombergMappingInfo symbol)
        {
            using (var writer = new StringWriter())
            {
                Serializer.Serialize(writer, symbol);
                writer.Flush();
                return writer.ToString();
            }
        }

        private static BloombergMappingInfo Deserialize(string json)
        {
            using (var reader = new JsonTextReader(new StringReader(json)))
            {
                return Serializer.Deserialize<BloombergMappingInfo>(reader);
            }
        }
    }
}