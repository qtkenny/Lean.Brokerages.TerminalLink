/*
* QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
* Lean Algorithmic Trading Engine v2.2 Copyright 2015 QuantConnect Corporation.
*/

using System;
using System.Linq;
using QuantConnect.Data;
using QuantConnect.Util;
using QuantConnect.Packets;
using QuantConnect.Interfaces;
using QuantConnect.Securities;
using QuantConnect.Brokerages;
using QuantConnect.Configuration;
using System.Collections.Generic;

namespace QuantConnect.TerminalLink
{
    /// <summary>
    /// Provides an implementations of <see cref="IBrokerageFactory"/> that produces a <see cref="TerminalLinkBrokerage"/>
    /// </summary>
    public class TerminalLinkBrokerageFactory : BrokerageFactory
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="TerminalLinkBrokerageFactory"/> class.
        /// </summary>
        public TerminalLinkBrokerageFactory()
            : base(typeof(TerminalLinkBrokerage))
        {
        }

        protected TerminalLinkBrokerageFactory(Type type) : base(type)
        {
        }

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        public override void Dispose()
        {
        }

        /// <summary>
        /// Gets the brokerage data required to run the brokerage from configuration/disk
        /// </summary>
        /// <remarks>
        /// The implementation of this property will create the brokerage data dictionary required for
        /// running live jobs. See <see cref="IJobQueueHandler.NextJob"/>
        /// </remarks>
        public override Dictionary<string, string> BrokerageData => new Dictionary<string, string>
        {
            { "terminal-link-api-type", Config.Get("terminal-link-api-type") },
            { "terminal-link-environment", Config.Get("terminal-link-environment") },
            { "terminal-link-server-host", Config.Get("terminal-link-server-host") },
            { "terminal-link-server-port", Config.Get("terminal-link-server-port") },
            { "terminal-link-symbol-map-file", Config.Get("terminal-link-symbol-map-file", "terminal-link-symbol-map.json") }
        };

        /// <summary>
        /// Gets a new instance of the <see cref="DefaultBrokerageModel"/>
        /// </summary>
        /// <param name="orderProvider">The order provider</param>
        public override IBrokerageModel GetBrokerageModel(IOrderProvider orderProvider) => new DefaultBrokerageModel();

        /// <summary>
        /// Creates a new <see cref="IBrokerage"/> instance
        /// </summary>
        /// <param name="job">The job packet to create the brokerage for</param>
        /// <param name="algorithm">The algorithm instance</param>
        /// <returns>A new brokerage instance</returns>
        public override IBrokerage CreateBrokerage(LiveNodePacket job, IAlgorithm algorithm)
        {
            var errors = new List<string>();

            // read values from the brokerage data
            var apiType = Read<ApiType>(job.BrokerageData, "terminal-link-api-type", errors);
            var environment = Read<Environment>(job.BrokerageData, "terminal-link-environment", errors);
            var serverHost = Read<string>(job.BrokerageData, "terminal-link-server-host", errors);
            var serverPort = Read<int>(job.BrokerageData, "terminal-link-server-port", errors);
            var symbolMapFile = Read<string>(job.BrokerageData, "terminal-link-symbol-map-file", errors);

            if (errors.Count != 0)
            {
                // if we had errors then we can't create the instance
                throw new Exception(string.Join(System.Environment.NewLine, errors));
            }

            var symbolMapper = Composer.Instance.GetExportedValues<ITerminalLinkSymbolMapper>().FirstOrDefault();
            if (symbolMapper == null)
            {
                symbolMapper = new TerminalLinkSymbolMapper(symbolMapFile);
                Composer.Instance.AddPart<ISymbolMapper>(symbolMapper);
            }

            var dataAggregator = Composer.Instance.GetExportedValueByTypeName<IDataAggregator>(
                Config.Get("data-aggregator", "QuantConnect.Lean.Engine.DataFeeds.AggregationManager"));

            var instance = CreateInstance(algorithm, apiType, environment, serverHost, serverPort, symbolMapper, dataAggregator);
            Composer.Instance.AddPart<IDataQueueHandler>(instance);
            return instance;
        }

        protected virtual TerminalLinkBrokerage CreateInstance(IAlgorithm algorithm, ApiType apiType, Environment environment,
            string serverHost, int serverPort, ITerminalLinkSymbolMapper symbolMapper, IDataAggregator aggregator)
        {
            return new TerminalLinkBrokerage(algorithm.Transactions, apiType, environment, serverHost, serverPort, symbolMapper, aggregator);
        }
    }
}
