using System;
using System.IO;
using System.Runtime.Serialization;
using Newtonsoft.Json;
using NUnit.Framework;
using QuantConnect.Bloomberg;

namespace QuantConnect.BloombergTests
{
    [TestFixture]
    public class BloombergSymbolTests
    {
        private static readonly JsonSerializer Serializer = new JsonSerializer();
        private readonly BloombergSymbol _underTest =
            new BloombergSymbol {Alias = "Alias1", Market = "Market1", ExpiryMonthYear = "Exp1", Underlying = "Underlying1", SecurityType = SecurityType.Cfd};

        [Test]
        public void Can_Serialize_And_Deserialize()
        {
            var json = Serialize(_underTest);
            Assert.NotNull(json);
            var actual = Deserialize(json);
            Assert.AreEqual(_underTest.Underlying, actual.Underlying);
            Assert.AreEqual(_underTest.Market, actual.Market);
            Assert.AreEqual(_underTest.ExpiryMonthYear, actual.ExpiryMonthYear);
            Assert.AreEqual(_underTest.SecurityType, actual.SecurityType);
            Assert.AreEqual(_underTest.Alias, actual.Alias);
        }

        private static string Serialize(BloombergSymbol symbol)
        {
            using (var writer = new StringWriter())
            {
                Serializer.Serialize(writer, symbol);
                writer.Flush();
                return writer.ToString();
            }
        }

        private static BloombergSymbol Deserialize(string json)
        {
            using (var reader = new JsonTextReader(new StringReader(json)))
            {
                return Serializer.Deserialize<BloombergSymbol>(reader);
            }
        }
    }
}