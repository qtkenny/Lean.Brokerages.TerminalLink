/*
* QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
* Lean Algorithmic Trading Engine v2.2 Copyright 2015 QuantConnect Corporation.
*/

namespace QuantConnect.Bloomberg
{
    /// <summary>
    /// The Bloomberg API types
    /// </summary>
    public enum ApiType
    {
        /// <summary>
        /// The Desktop API
        /// </summary>
        Desktop,

        /// <summary>
        /// The Server-side API
        /// </summary>
        Server,

        /// <summary>
        /// The BPIPE API
        /// </summary>
        Bpipe
    }

    /// <summary>
    /// The Bloomberg environment types for EMSX API
    /// </summary>
    public enum Environment
    {
        /// <summary>
        /// The Production environment
        /// </summary>
        Production,

        /// <summary>
        /// The Beta environment
        /// </summary>
        Beta
    }

    /// <summary>
    /// The Bloomberg service types
    /// </summary>
    public enum ServiceType
    {
        /// <summary>
        /// The market data service
        /// </summary>
        MarketData,

        /// <summary>
        /// The reference data service
        /// </summary>
        ReferenceData,

        /// <summary>
        /// The EMS service
        /// </summary>
        Ems
    }

    /// <summary>
    /// Event Status Messages
    /// </summary>
    public enum EventStatus
    {
        /// <summary>
        /// Heartbeat Message, HB_MESSAGE
        /// </summary>
        Heartbeat = 1,

        /// <summary>
        /// Initial Paint Message on all subscription fields, INIT_PAINT
        /// </summary>
        InitialPaint = 4,

        /// <summary>
        /// New Order or Route Message on all subscription fields, NEW_ORDER_ROUTE
        /// </summary>
        New = 6,

        /// <summary>
        /// This field dynamically updates for existing Order and route, UPD_ORDER_ROUTE
        /// </summary>
        Update = 7,

        /// <summary>
        /// Order and route deletion message, DELETION_MESSAGE
        /// </summary>
        Delete = 8,

        /// <summary>
        /// The end of the initial paint message, INIT_PAINT_END
        /// </summary>
        EndPaint = 11
    }
}
