using System;
using System.Linq;
using System.Text;
using CacheManager.Core.Internal;
using StackExchange.Redis;
using static CacheManager.Core.Utility.Guard;

namespace CacheManager.Redis
{
    /// <summary>
    /// 
    /// </summary>
    internal class RedisMinimalValueConverter : IRedisValueConverter
    {
        private readonly ICacheSerializer _serializer;

        public RedisMinimalValueConverter(ICacheSerializer serializer)
        {
            NotNull(serializer, nameof(serializer));

            _serializer = serializer;
        }

        class Mapping
        {
            public Type Type;
            public string Marker;
            public Func<byte[], object> Parse;
            public Func<object, RedisValue> Convert;

            protected byte[] MarkerBytes;

            public bool Match(byte[] type)
            {
                return MarkerBytes.SequenceEqual(type);
            }
        }

        class Mapping<T> : Mapping
        {
            public Mapping(string marker, Func<string, byte[], object> parse, Func<T, RedisValue> convert)
            {
                Type = typeof(T);
                Marker = "(" + marker + ")";
                MarkerBytes = Encoding.UTF8.GetBytes(Marker);
                Parse = (byte[] v) => parse(Encoding.ASCII.GetString(v), v);

                var redisMark = (RedisValue)Marker;
                Convert = (object v) => redisMark + convert((T)v);
            }
        }

        Mapping[] _mapping = new Mapping[]
        {
                new Mapping<bool>("bool", (s,b) => (s[0] == '1' || s[0] == 'T'), x => x),
                new Mapping<byte>("byte", (s,b) => byte.Parse(s), x => x),
                new Mapping<byte[]>("byte[]", (s,b) => b, x => x),
                new Mapping<string>("string", (s,b) => s, x => x),
                new Mapping<short>("short", (s,b) => Int16.Parse(s), x => x),
                new Mapping<ushort>("ushort", (s,b) => UInt16.Parse(s), x => x),
                new Mapping<int>("int", (s,b) => Int32.Parse(s), x => x),
                new Mapping<uint>("uint", (s,b) => UInt32.Parse(s), x => x),
                new Mapping<long>("long", (s,b) => Int64.Parse(s), x => x),
                // ulong can exceed the supported lenght of storing integers (which is signed 64bit integer)
                // also, even if we do not exceed long.MaxValue, the SA client stores it as double for no aparent reason => cast to long fixes it.
                new Mapping<ulong>("ulong", (s,b) => UInt64.Parse(s), x => x > long.MaxValue ? (RedisValue)x.ToString() : checked((long)x)),
                new Mapping<char>("char", (s,b) => Encoding.UTF8.GetChars(b)[0], x => Encoding.UTF8.GetBytes(x.ToString())),
                new Mapping<float>("float", (s,b) => Single.Parse(s), x => x),
                new Mapping<double>("double", (s,b) => Double.Parse(s), x => x),
        };

        object ConvertFromRedisValue(RedisValue value)
        {
            byte[] bytes = value;
            if (bytes[0] != '(')
                throw new InvalidCastException("Unknown type: " + value.ToString());

            var end = Array.IndexOf(bytes, (byte)')') + 1;
            var typeName = new ArraySegment<byte>(bytes, 0, end).ToArray();
            var start = end;
            var content = new ArraySegment<byte>(bytes, start, bytes.Length - start).ToArray();

            foreach (var x in _mapping)
            {
                if (x.Match(typeName))
                {
                    return x.Parse(content);
                }
            }

            var type = TypeCache.GetType(Encoding.ASCII.GetString(typeName).TrimStart('(').TrimEnd(')'));
            EnsureNotNull(type, "Type could not be loaded, {0}.", type);

            return _serializer.Deserialize(content, type);
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "0", Scope = "member", Target = "CacheManager.Redis.RedisValueConverter.#CacheManager.Redis.IRedisValueConverter`1<System.Object>.ToRedisValue(System.Object)", Justification = "For performance reasons we don't do checks at this point. Also, its internally used only.")]
        RedisValue ConvertToRedisValue(object value)
        {
            var valueType = value.GetType();

            var mapping = _mapping.Where(x => x.Type == valueType).FirstOrDefault();
            if (mapping != null)
            {
                return mapping.Convert(value);
            }

            return (RedisValue)$"({valueType})" + (RedisValue)_serializer.Serialize(value);
        }

        public RedisValue ToRedisValue<T>(T value)
        {
            return ConvertToRedisValue(value);
        }

        public T FromRedisValue<T>(RedisValue value)
        {
            return (T)ConvertFromRedisValue(value);
        }

        public T FromRedisValue<T>(RedisValue value, string valueType)
        {
            return (T)ConvertFromRedisValue(value);
        }
    }
}