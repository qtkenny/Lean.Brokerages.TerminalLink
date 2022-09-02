/*
* QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
* Lean Algorithmic Trading Engine v2.2 Copyright 2015 QuantConnect Corporation.
*/

using System;
using Bloomberglp.Blpapi;

namespace QuantConnect.TerminalLink
{
    public class SchemaFieldDefinition
    {
        public string Name { get; }
        public Schema.Status Status { get; }
        public Schema.Datatype DataType { get; }
        public string Description { get; }

        public SchemaFieldDefinition(SchemaElementDefinition e)
        {
            Name = e.Name.ToString();
            Status = e.Status;
            DataType = e.TypeDefinition.Datatype;
            Description = e.Description;
        }

        public bool IsOrderField()
        {
            return Description.IndexOf("Order", StringComparison.Ordinal) > -1 ||
                   Description.IndexOf("O,R", StringComparison.Ordinal) > -1;
        }

        public bool IsRouteField()
        {
            return Description.IndexOf("Route", StringComparison.Ordinal) > -1 ||
                   Description.IndexOf("O,R", StringComparison.Ordinal) > -1;
        }

        public bool IsStatic()
        {
            return Description.IndexOf("Static", StringComparison.Ordinal) > -1;
        }
    }
}
