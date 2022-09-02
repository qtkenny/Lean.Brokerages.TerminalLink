/*
* QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
* Lean Algorithmic Trading Engine v2.2 Copyright 2015 QuantConnect Corporation.
*/

using System.IO;
using Newtonsoft.Json;
using NUnit.Framework;
using QuantConnect.TerminalLink;

namespace QuantConnect.TerminalLinkTests
{
    [TestFixture]
    public class TerminalLinkMappingInfoTests
    {
        private static readonly JsonSerializer Serializer = new JsonSerializer();
        private readonly TerminalLinkMappingInfo _underTest =
            new TerminalLinkMappingInfo { Alias = "Alias1", Market = "Market1", Underlying = "Underlying1", SecurityType = SecurityType.Cfd, TickerSuffix = "1", RootLookupSuffix = "A"};

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

        private static string Serialize(TerminalLinkMappingInfo symbol)
        {
            using (var writer = new StringWriter())
            {
                Serializer.Serialize(writer, symbol);
                writer.Flush();
                return writer.ToString();
            }
        }

        private static TerminalLinkMappingInfo Deserialize(string json)
        {
            using (var reader = new JsonTextReader(new StringReader(json)))
            {
                return Serializer.Deserialize<TerminalLinkMappingInfo>(reader);
            }
        }
    }
}