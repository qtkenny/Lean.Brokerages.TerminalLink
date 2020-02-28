/*
* QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
* Lean Algorithmic Trading Engine v2.2 Copyright 2015 QuantConnect Corporation.
*/

using System.Collections;
using System.Collections.Generic;
using Bloomberglp.Blpapi;

namespace QuantConnect.Bloomberg
{
    public class BloombergFields : IEnumerable<BloombergField>
    {
        private readonly SchemaFieldDefinitions _fieldDefinitions;
        private readonly Dictionary<string, BloombergField> _fields = new Dictionary<string, BloombergField>();

        public BloombergFields(SchemaFieldDefinitions fieldDefinitions)
        {
            _fieldDefinitions = fieldDefinitions;

            LoadFields();
        }

        private void LoadFields()
        {
            foreach (var sdf in _fieldDefinitions)
            {
                _fields.Add(sdf.Name, new BloombergField(sdf.Name, string.Empty));
            }
        }

        public void PopulateFields(Message message, bool dynamicFieldsOnly)
        {
            var fieldCount = message.NumElements;

            var e = message.AsElement;

            for (var i = 0; i < fieldCount; i++)
            {
                var load = true;

                var f = e.GetElement(i);

                var fieldName = f.Name.ToString();

                if (dynamicFieldsOnly)
                {
                    var sfd = _fieldDefinitions.FindSchemaFieldByName(fieldName);
                    if (sfd != null && sfd.IsStatic())
                    {
                        load = false;
                    }
                }

                if (load)
                {
                    var fd = GetField(fieldName);
                    fd?.SetCurrentValue(f.GetValueAsString());
                }
            }
        }

        public BloombergField GetField(string name)
        {
            BloombergField field;

            return _fields.TryGetValue(name, out field) ? field : null;
        }

        public IEnumerator<BloombergField> GetEnumerator()
        {
            return _fields.Values.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        internal IReadOnlyDictionary<string, BloombergField> Contents => _fields;
    }
}
