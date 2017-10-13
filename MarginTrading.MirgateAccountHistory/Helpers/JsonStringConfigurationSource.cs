using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using Microsoft.Extensions.Configuration;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace MarginTrading.MirgateAccountHistory.Helpers
{
    public class JsonStringConfigurationSource : IConfigurationSource
    {
        public JsonStringConfigurationSource(string content)
        {
            Content = content;
        }

        public string Content { get; }

        public IConfigurationProvider Build(IConfigurationBuilder builder)
        {
            return new JsonStringConfigurationProvider(this);
        }

        public class JsonStringConfigurationProvider : ConfigurationProvider
        {
            private readonly JsonStringConfigurationSource _source;

            public JsonStringConfigurationProvider(JsonStringConfigurationSource source)
            {
                _source = source;
            }

            public override void Load()
            {
                foreach (var setting in JsonConfigurationFileParser.Parse(_source.Content))
                {
                    Data[setting.Key] = setting.Value;
                }
            }

            private class JsonConfigurationFileParser
            {
                private readonly IDictionary<string, string> _data = new SortedDictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                private readonly Stack<string> _context = new Stack<string>();
                private string _currentPath;
                private JsonTextReader _reader;

                private JsonConfigurationFileParser() { }

                public static IDictionary<string, string> Parse(string input)
                {
                    var parser = new JsonConfigurationFileParser();
                    parser._reader = new JsonTextReader(new StringReader(input)) { DateParseHandling = DateParseHandling.None };
                    parser.VisitJObject(JObject.Load(parser._reader));
                    return parser._data;
                }

                private void VisitJObject(JObject jObject)
                {
                    foreach (var property in jObject.Properties())
                    {
                        EnterContext(property.Name);
                        VisitProperty(property);
                        ExitContext();
                    }
                }

                private void VisitProperty(JProperty property)
                {
                    VisitToken(property.Value);
                }

                private void VisitToken(JToken token)
                {
                    switch (token.Type)
                    {
                        case JTokenType.Object:
                            VisitJObject(token.Value<JObject>());
                            break;

                        case JTokenType.Array:
                            VisitArray(token.Value<JArray>());
                            break;

                        case JTokenType.Integer:
                        case JTokenType.Float:
                        case JTokenType.String:
                        case JTokenType.Boolean:
                        case JTokenType.Bytes:
                        case JTokenType.Raw:
                        case JTokenType.Null:
                            VisitPrimitive(token.Value<JValue>());
                            break;

                        default:
                            throw new FormatException($"Unsupported Json token: path: {_reader.Path}, type: {_reader.TokenType}, line: {_reader.LineNumber}, pos: {_reader.LinePosition}");
                    }
                }

                private void VisitArray(JArray array)
                {
                    for (int index = 0; index < array.Count; index++)
                    {
                        EnterContext(index.ToString());
                        VisitToken(array[index]);
                        ExitContext();
                    }
                }

                private void VisitPrimitive(JValue data)
                {
                    var key = _currentPath;

                    if (_data.ContainsKey(key))
                    {
                        throw new FormatException($"Key {key} is duplicated in the config");
                    }
                    _data[key] = data.ToString(CultureInfo.InvariantCulture);
                }

                private void EnterContext(string context)
                {
                    _context.Push(context);
                    _currentPath = ConfigurationPath.Combine(_context.Reverse());
                }

                private void ExitContext()
                {
                    _context.Pop();
                    _currentPath = ConfigurationPath.Combine(_context.Reverse());
                }
            }
        }
    }

}
