/*
* QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
* Lean Algorithmic Trading Engine v2.2 Copyright 2015 QuantConnect Corporation.
*/

using System.Collections;
using System.Collections.Generic;

namespace QuantConnect.TerminalLink
{
    public class SchemaFieldDefinitions : IEnumerable<SchemaFieldDefinition>
    {
        private readonly Dictionary<string, SchemaFieldDefinition> _fields = new Dictionary<string, SchemaFieldDefinition>();

        public void Add(SchemaFieldDefinition field)
        {
            _fields.Add(field.Name, field);
        }

        public void Clear()
        {
            _fields.Clear();
        }

        public IEnumerator<SchemaFieldDefinition> GetEnumerator()
        {
            return _fields.Values.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}
