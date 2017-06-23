using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using CacheManager.Core;
using CacheManager.Core.Internal;
using CacheManager.Core.Logging;
using StackExchange.Redis;
using static CacheManager.Core.Utility.Guard;

namespace CacheManager.Redis
{
    /// <summary>
    /// Cache handle implementation for Redis.
    /// </summary>
    /// <typeparam name="TCacheValue">The type of the cache value.</typeparam>
    [RequiresSerializer]
    public class RedisMinimalCacheHandle<TCacheValue> : BaseCacheHandle<TCacheValue>
    {
        private static readonly TimeSpan MinimumExpirationTimeout = TimeSpan.FromMilliseconds(1);
        
        private readonly ICacheManagerConfiguration _managerConfiguration;
        private readonly RedisMinimalValueConverter _valueConverter;
        private readonly RedisConnectionManager _connection;
        private RedisConfiguration _redisConfiguration = null;

        private object _lockObject = new object();
        private ICacheSerializer _serializer;

        /// <summary>
        /// Initializes a new instance of the <see cref="RedisCacheHandle{TCacheValue}"/> class.
        /// </summary>
        /// <param name="managerConfiguration">The manager configuration.</param>
        /// <param name="configuration">The cache handle configuration.</param>
        /// <param name="loggerFactory">The logger factory.</param>
        /// <param name="serializer">The serializer.</param>
        public RedisMinimalCacheHandle(ICacheManagerConfiguration managerConfiguration, CacheHandleConfiguration configuration, ILoggerFactory loggerFactory, ICacheSerializer serializer)
            : base(managerConfiguration, configuration)
        {
            NotNull(loggerFactory, nameof(loggerFactory));
            NotNull(managerConfiguration, nameof(managerConfiguration));
            NotNull(configuration, nameof(configuration));
            EnsureNotNull(serializer, "A serializer is required for the redis cache handle");

            Logger = loggerFactory.CreateLogger(this);
            _managerConfiguration = managerConfiguration;
            _serializer = serializer;
            _redisConfiguration = RedisConfigurations.GetConfiguration(configuration.Key);
            _connection = new RedisConnectionManager(_redisConfiguration, loggerFactory);
            _valueConverter = new RedisMinimalValueConverter(serializer);

            if (_redisConfiguration.KeyspaceNotificationsEnabled)
            {
                // notify-keyspace-events needs to be set to "Exe" at least! Otherwise we will not receive any events.
                // this must be configured per server and should probably not be done automagically as this needs admin rights!
                // Let's try to check at least if those settings are configured (the check also works only if useAdmin is set to true though).
                try
                {
                    var configurations = _connection.GetConfiguration("notify-keyspace-events");
                    foreach (var cfg in configurations)
                    {
                        if (!cfg.Value.Contains("E"))
                        {
                            Logger.LogWarn("Server {0} is missing configuration value 'E' in notify-keyspace-events to enable keyevents.", cfg.Key);
                        }

                        if (!(cfg.Value.Contains("A") ||
                            (cfg.Value.Contains("x") && cfg.Value.Contains("e"))))
                        {
                            Logger.LogWarn("Server {0} is missing configuration value 'A' or 'x' and 'e' in notify-keyspace-events to enable keyevents for expired and evicted keys.", cfg.Key);
                        }
                    }
                }
                catch
                {
                    Logger.LogDebug("Could not read configuration from redis to validate notify-keyspace-events. Most likely useAdmin is not set to true.");
                }

                SubscribeKeyspaceNotifications();
            }
        }

        /// <inheritdoc />
        public override bool IsDistributedCache
        {
            get
            {
                return true;
            }
        }

        /// <summary>
        /// Gets the number of items the cache handle currently maintains.
        /// </summary>
        /// <value>The count.</value>
        /// <exception cref="System.InvalidOperationException">No active master found.</exception>
        public override int Count
        {
            get
            {
                if (_redisConfiguration.TwemproxyEnabled)
                {
                    Logger.LogWarn("'Count' cannot be calculated. Twemproxy mode is enabled which does not support accessing the servers collection.");
                    return 0;
                }

                var count = 0;
                foreach (var server in Servers.Where(p => !p.IsSlave && p.IsConnected))
                {
                    count += (int)server.DatabaseSize(_redisConfiguration.Database);
                }

                // approx size, only size on the master..
                return count;
            }
        }

#pragma warning disable CS3003 // Type is not CLS-compliant

        /// <summary>
        /// Gets the servers.
        /// </summary>
        /// <value>The list of servers.</value>
        public IEnumerable<IServer> Servers => _connection.Servers;

        /// <summary>
        /// Gets the features the redis server supports.
        /// </summary>
        /// <value>The server features.</value>
        public RedisFeatures Features => _connection.Features;

#pragma warning restore CS3003 // Type is not CLS-compliant

        /// <inheritdoc />
        protected override ILogger Logger { get; }

        /// <summary>
        /// Clears this cache, removing all items in the base cache and all regions.
        /// </summary>
        public override void Clear()
        {
            try
            {
                foreach (var server in Servers.Where(p => !p.IsSlave))
                {
                    Retry(() =>
                    {
                        if (server.IsConnected)
                        {
                            server.FlushDatabase(_redisConfiguration.Database);
                        }
                    });
                }
            }
            catch (NotSupportedException ex)
            {
                throw new NotSupportedException($"Clear is not available because '{ex.Message}'", ex);
            }
        }

        /// <summary>
        /// Clears the cache region, removing all items from the specified <paramref name="region"/> only.
        /// </summary>
        /// <param name="region">The cache region.</param>
        public override void ClearRegion(string region)
        {
            Retry(() =>
            {
                // we are storing all keys stored in the region in the hash for key=region
                var hashKeys = _connection.Database.HashKeys(region);

                if (hashKeys.Length > 0)
                {
                    // lets remove all keys which where in the region
                    // 01/32/16 changed to remove one by one because on clusters the keys could belong to multiple slots
                    foreach (var key in hashKeys.Where(p => p.HasValue))
                    {
                        _connection.Database.KeyDelete(key.ToString(), CommandFlags.FireAndForget);
                    }
                }

                // now delete the region
                _connection.Database.KeyDelete(region);
            });
        }

        /// <inheritdoc />
        public override bool Exists(string key)
        {
            var fullKey = GetKey(key);
            return Retry(() => _connection.Database.KeyExists(fullKey));
        }

        /// <inheritdoc />
        public override bool Exists(string key, string region)
        {
            NotNullOrWhiteSpace(region, nameof(region));

            var fullKey = GetKey(key, region);
            return Retry(() => _connection.Database.KeyExists(fullKey));
        }

        /// <inheritdoc />
        public override IEnumerable<string> Keys(string pattern, string region)
        {
            var keyPattern = GetKey(pattern, region);

            // Keys are spread out across the cluster, slaves should contain a complete copy of their master nodes, so ignore them.
            return _connection
                .Servers
                .Where(s => s.IsConnected && !s.IsSlave)
                .SelectMany(s => s.Keys(_redisConfiguration.Database, keyPattern).Select(k => k.ToString()))
                .Select(k => ParseKey(k).Item1);
        }

        /// <inheritdoc />
        public override UpdateItemResult<TCacheValue> Update(string key, Func<TCacheValue, TCacheValue> updateValue, int maxRetries)
            => Update(key, null, updateValue, maxRetries);

        /// <inheritdoc />
        public override UpdateItemResult<TCacheValue> Update(string key, string region, Func<TCacheValue, TCacheValue> updateValue, int maxRetries)
        {
            var committed = false;
            var tries = 0;
            var fullKey = GetKey(key, region);

            return Retry(() =>
            {
                do
                {
                    tries++;

                    var item = GetCacheItemInternal(key, region);

                    if (item == null)
                    {
                        return UpdateItemResult.ForItemDidNotExist<TCacheValue>();
                    }

                    ValidateExpirationTimeout(item);

                    var oldValue = ToRedisValue(item.Value);

                    var tran = _connection.Database.CreateTransaction();
                    tran.AddCondition(Condition.StringEqual(fullKey, oldValue));

                    // run update
                    var newValue = updateValue(item.Value);

                    // added null check, throw explicit to me more consistent. Otherwise it would throw later
                    if (newValue == null)
                    {
                        return UpdateItemResult.ForFactoryReturnedNull<TCacheValue>();
                    }

                    tran.StringSetAsync(fullKey, ToRedisValue(newValue));

                    committed = tran.Execute();

                    if (committed)
                    {
                        var newItem = item.WithValue(newValue);
                        newItem.LastAccessedUtc = DateTime.UtcNow;

                        return UpdateItemResult.ForSuccess(newItem, tries > 1, tries);
                    }

                    Logger.LogDebug("Update of {0} {1} failed with version conflict, retrying {2}/{3}", key, region, tries, maxRetries);
                }
                while (committed == false && tries <= maxRetries);

                return UpdateItemResult.ForTooManyRetries<TCacheValue>(tries);
            });
        }

#pragma warning restore CS1591
#pragma warning restore SA1600

        /// <summary>
        /// Adds a value to the cache.
        /// <para>
        /// Add call is synced, so might be slower than put which is fire and forget but we want to
        /// return true|false if the operation was successfully or not. And always returning true
        /// could be misleading if the item already exists
        /// </para>
        /// </summary>
        /// <param name="item">The <c>CacheItem</c> to be added to the cache.</param>
        /// <returns>
        /// <c>true</c> if the key was not already added to the cache, <c>false</c> otherwise.
        /// </returns>
        protected override bool AddInternalPrepared(CacheItem<TCacheValue> item) =>
            Retry(() => Set(item, When.NotExists, true));

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting
        /// unmanaged resources.
        /// </summary>
        /// <param name="disposeManaged">Indicator if managed resources should be released.</param>
        protected override void Dispose(bool disposeManaged)
        {
            base.Dispose(disposeManaged);
            if (disposeManaged)
            {
                // this.connection.RemoveConnection();
            }
        }

        /// <summary>
        /// Gets a <c>CacheItem</c> for the specified key.
        /// </summary>
        /// <param name="key">The key being used to identify the item within the cache.</param>
        /// <returns>The <c>CacheItem</c>.</returns>
        protected override CacheItem<TCacheValue> GetCacheItemInternal(string key)
            => GetCacheItemInternal(key, null);

        /// <summary>
        /// Gets a <c>CacheItem</c> for the specified key.
        /// </summary>
        /// <param name="key">The key being used to identify the item within the cache.</param>
        /// <param name="region">The cache region.</param>
        /// <returns>The <c>CacheItem</c>.</returns>
        protected override CacheItem<TCacheValue> GetCacheItemInternal(string key, string region)
        {
            return GetCacheItemAndVersion(key, region);
        }

        private CacheItem<TCacheValue> GetCacheItemAndVersion(string key, string region)
        {
            return Retry(() =>
            {
                var fullKey = GetKey(key, region);

                var valueWithExpire = _connection.Database.StringGetWithExpiry(fullKey);
                var value = FromRedisValue(valueWithExpire.Value);

                if (valueWithExpire.Expiry == null)
                {
                    return new CacheItem<TCacheValue>(key, value, ExpirationMode.None, TimeSpan.MaxValue);
                }
                else
                {
                    return new CacheItem<TCacheValue>(key, value, ExpirationMode.Absolute, valueWithExpire.Expiry.Value);
                }
            });
        }

#pragma warning restore CS1591 // Missing XML comment for publicly visible type or member
#pragma warning restore SA1600

        /// <summary>
        /// Puts the <paramref name="item"/> into the cache. If the item exists it will get updated
        /// with the new value. If the item doesn't exist, the item will be added to the cache.
        /// </summary>
        /// <param name="item">The <c>CacheItem</c> to be added to the cache.</param>
        protected override void PutInternal(CacheItem<TCacheValue> item)
            => base.PutInternal(item);

        /// <summary>
        /// Puts the <paramref name="item"/> into the cache. If the item exists it will get updated
        /// with the new value. If the item doesn't exist, the item will be added to the cache.
        /// </summary>
        /// <param name="item">The <c>CacheItem</c> to be added to the cache.</param>
        protected override void PutInternalPrepared(CacheItem<TCacheValue> item) =>
            Retry(() => Set(item, When.Always, false));

        /// <summary>
        /// Removes a value from the cache for the specified key.
        /// </summary>
        /// <param name="key">The key being used to identify the item within the cache.</param>
        /// <returns>
        /// <c>true</c> if the key was found and removed from the cache, <c>false</c> otherwise.
        /// </returns>
        protected override bool RemoveInternal(string key) => RemoveInternal(key, null);

#pragma warning disable CSE0003

        /// <summary>
        /// Removes a value from the cache for the specified key.
        /// </summary>
        /// <param name="key">The key being used to identify the item within the cache.</param>
        /// <param name="region">The cache region.</param>
        /// <returns>
        /// <c>true</c> if the key was found and removed from the cache, <c>false</c> otherwise.
        /// </returns>
        protected override bool RemoveInternal(string key, string region)
        {
            return Retry(() =>
            {
                var fullKey = GetKey(key, region);

                // clean up region
                if (!string.IsNullOrWhiteSpace(region))
                {
                    _connection.Database.HashDelete(region, fullKey, CommandFlags.FireAndForget);
                }

                // remove key
                var result = _connection.Database.KeyDelete(fullKey);

                return result;
            });
        }

        private void SubscribeKeyspaceNotifications()
        {
            _connection.Subscriber.Subscribe(
                 $"__keyevent@{_redisConfiguration.Database}__:expired",
                 (channel, key) =>
                 {
                     var tupple = ParseKey(key);
                     if (Logger.IsEnabled(LogLevel.Debug))
                     {
                         Logger.LogDebug("Got expired event for key '{0}:{1}'", tupple.Item2, tupple.Item1);
                     }

                     // we cannot return the original value here because we don't have it
                     TriggerCacheSpecificRemove(tupple.Item1, tupple.Item2, CacheItemRemovedReason.Expired, null);
                 });

            _connection.Subscriber.Subscribe(
                $"__keyevent@{_redisConfiguration.Database}__:evicted",
                (channel, key) =>
                {
                    var tupple = ParseKey(key);
                    if (Logger.IsEnabled(LogLevel.Debug))
                    {
                        Logger.LogDebug("Got evicted event for key '{0}:{1}'", tupple.Item2, tupple.Item1);
                    }

                    // we cannot return the original value here because we don't have it
                    TriggerCacheSpecificRemove(tupple.Item1, tupple.Item2, CacheItemRemovedReason.Evicted, null);
                });

            _connection.Subscriber.Subscribe(
                $"__keyevent@{_redisConfiguration.Database}__:del",
                (channel, key) =>
                {
                    var tupple = ParseKey(key);
                    if (Logger.IsEnabled(LogLevel.Debug))
                    {
                        Logger.LogDebug("Got del event for key '{0}:{1}'", tupple.Item2, tupple.Item1);
                    }

                    // we cannot return the original value here because we don't have it
                    TriggerCacheSpecificRemove(tupple.Item1, tupple.Item2, CacheItemRemovedReason.ExternalDelete, null);
                });
        }

#pragma warning restore CSE0003

        private static Tuple<string, string> ParseKey(string value)
        {
            if (value == null)
            {
                return Tuple.Create<string, string>(null, null);
            }

            var sepIndex = value.IndexOf(':');
            var hasRegion = sepIndex > 0;
            var key = value;
            string region = null;

            if (hasRegion)
            {
                region = value.Substring(0, sepIndex);
                key = value.Substring(sepIndex + 1);
            }

            return Tuple.Create(key, region);
        }

        private static void ValidateExpirationTimeout(CacheItem<TCacheValue> item)
        {
            if (item.ExpirationMode == ExpirationMode.Sliding)
                throw new NotSupportedException("Sliding mode not supported");

            if ((item.ExpirationMode == ExpirationMode.Absolute) && item.ExpirationTimeout < MinimumExpirationTimeout)
            {
                throw new ArgumentException("Timeout lower than one millisecond is not supported.", nameof(item.ExpirationTimeout));
            }
        }

        private string GetKey(string key, string region = null)
        {
            if (string.IsNullOrWhiteSpace(key))
            {
                throw new ArgumentNullException(nameof(key));
            }

            var fullKey = key;

            if (!string.IsNullOrWhiteSpace(region))
            {
                fullKey = string.Concat(region, ":", key);
            }

            return fullKey;
        }

        private TCacheValue FromRedisValue(RedisValue value)
        {
            if (value.IsNull || value.IsNullOrEmpty || !value.HasValue)
            {
                return default(TCacheValue);
            }

            return _valueConverter.FromRedisValue<TCacheValue>(value);
        }

        private RedisValue ToRedisValue(TCacheValue value)
        {
            return _valueConverter.ToRedisValue(value);
        }

        private T Retry<T>(Func<T> retryme) =>
            RetryHelper.Retry(retryme, _managerConfiguration.RetryTimeout, _managerConfiguration.MaxRetries, Logger);

        private void Retry(Action retryme)
            => Retry(
                () =>
                {
                    retryme();
                    return true;
                });

        private bool Set(CacheItem<TCacheValue> item, When when, bool sync = false)
        {
            return Retry(() =>
            {
                var fullKey = GetKey(item.Key, item.Region);
                var value = ToRedisValue(item.Value);

                ValidateExpirationTimeout(item);

                var flags = sync ? CommandFlags.None : CommandFlags.FireAndForget;

                var setResult = _connection.Database.StringSet(fullKey, value, item.ExpirationTimeout, when, flags);

                // setResult from fire and forget is alwys false, so we have to assume it works...
                setResult = flags == CommandFlags.FireAndForget ? true : setResult;

                if (setResult)
                {
                    if (!string.IsNullOrWhiteSpace(item.Region))
                    {
                        // setting region lookup key if region is being used
                        _connection.Database.HashSet(item.Region, fullKey, "regionKey", When.Always, CommandFlags.FireAndForget);
                    }

                    if (item.ExpirationMode != ExpirationMode.None && item.ExpirationMode != ExpirationMode.Default)
                    {
                        _connection.Database.KeyExpire(fullKey, item.ExpirationTimeout, CommandFlags.FireAndForget);
                    }
                    else
                    {
                        // bugfix #9
                        _connection.Database.KeyPersist(fullKey, CommandFlags.FireAndForget);
                    }
                }

                return setResult;
            });
        }
    }
}