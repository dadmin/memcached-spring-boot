/*
 * Copyright 2017 Sixhours.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.sixhours.memcached.cache;

import net.spy.memcached.MemcachedClient;
import org.springframework.cache.support.AbstractValueAdaptingCache;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

/**
 * Cache implementation on top of Memcached.
 * Added support collections of keys. It always return values that were found
 *
 * @author Igor Bolic
 */
public class MemcachedCache extends AbstractValueAdaptingCache {

    private final MemcachedClient memcachedClient;
    private final MemcachedKeyGenerator memcachedKeyGenerator;
    private final int expiration;

    private final Lock lock = new ReentrantLock();

    private final AtomicLong hits = new AtomicLong();
    private final AtomicLong misses = new AtomicLong();

    /**
     * Create an {@code MemcachedCache} with the given settings.
     *
     * @param name            Cache name
     * @param memcachedClient {@link MemcachedClient}
     * @param expiration      Cache expiration in seconds
     * @param prefix          Cache key prefix
     */
    public MemcachedCache(String name, MemcachedClient memcachedClient, int expiration, String prefix) {
        super(true);
        this.memcachedClient = memcachedClient;
        this.memcachedKeyGenerator = new MemcachedKeyGenerator(prefix, name, Default.KEY_DELIMITER);
        this.expiration = expiration;
    }

    @Override
    protected Object lookup(Object key) {
        if (key instanceof Collection) {
            Map<String, String> keyToMemcachedKey = (Map<String, String>) ((Collection) key).stream().collect(Collectors.toMap(k -> k, memcachedKeyGenerator::memcachedKey));
            Map<String, Object> values = memcachedClient.getBulk(keyToMemcachedKey.values());
            trackCollectionHitsMisses(values.size(), ((Collection) key).size() - values.size());
            //replace memcached keys with origin keys in result
            keyToMemcachedKey.entrySet().forEach(entry -> {
                Object value = values.remove(entry.getValue());
                if (value != null) values.put(entry.getKey(), value);
            });
            return values;
        }
        return trackHitsMisses(memcachedClient.get(memcachedKeyGenerator.memcachedKey(key)));
    }

    @Override
    public String getName() {
        return this.memcachedKeyGenerator.getCacheName();
    }

    @Override
    public net.spy.memcached.MemcachedClient getNativeCache() {
        return this.memcachedClient;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T get(Object key, Callable<T> valueLoader) {
        Object value = lookup(key);
        if (value != null) {
            return (T) fromStoreValue(value);
        }

        lock.lock();
        try {
            value = lookup(key);
            if (value != null) {
                return (T) fromStoreValue(value);
            } else {
                return (T) fromStoreValue(loadValue(key, valueLoader));
            }
        } finally {
            lock.unlock();
        }
    }

    private <T> T loadValue(Object key, Callable<T> valueLoader) {
        T value;
        try {
            value = valueLoader.call();
        } catch (Exception e) {
            throw new ValueRetrievalException(key, valueLoader, e);
        }
        put(key, value);
        return value;
    }

    @Override
    public void put(Object key, Object value) {
        this.memcachedClient.set(memcachedKeyGenerator.memcachedKey(key), expiration, toStoreValue(value));
    }

    @Override
    public ValueWrapper putIfAbsent(Object key, Object value) {
        Object existingValue = lookup(key);
        if (existingValue == null) {
            put(key, value);
            return toValueWrapper(value);
        }

        return toValueWrapper(existingValue);
    }

    @Override
    public void evict(Object key) {
        this.memcachedClient.delete(memcachedKeyGenerator.memcachedKey(key));
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException("This implementations doesn't support cache clear");
    }

    public long hits() {
        return hits.get();
    }

    public long misses() {
        return misses.get();
    }

    /**
     * Tracks number of hits and misses per {@code MemcachedCache} instance.
     *
     * @param value Value returned from the underlying cache store.
     * @return The value
     */
    private Object trackHitsMisses(Object value) {
        if (value != null) {
            hits.incrementAndGet();
        } else {
            misses.incrementAndGet();
        }
        return value;
    }

    /**
     * Tracks number of hits and misses per {@code MemcachedCache} instance for collection key.
     *
     * @param hitsCount   how much keys were presented in cache
     * @param missesCount how much keys weren't presented in cache
     */
    private void trackCollectionHitsMisses(int hitsCount, int missesCount) {
        hits.addAndGet(hitsCount);
        misses.addAndGet(missesCount);
    }

    /**
     * Class for generating key fro memcached
     */
    public class MemcachedKeyGenerator {

        private String cachePrefix;
        private String cacheName;
        private String delimiter;

        public MemcachedKeyGenerator(String cachePrefix, String cacheName, String delimiter) {
            this.cachePrefix = cachePrefix;
            this.cacheName = cacheName;
            this.delimiter = delimiter;
        }

        public MemcachedKeyGenerator(String cacheName) {
            this.cacheName = cacheName;
            this.cachePrefix = Default.PREFIX;
            this.delimiter = Default.KEY_DELIMITER;
        }

        /**
         * Generate key for memcached as a string
         * @param key generic spring cache abstraction key. It cannot be used for memcache raw because for memcache
         *            string keys are required
         * @return string representation of the key
         */
        public String memcachedKey(Object key) {
            return cachePrefix + delimiter + cacheName + delimiter + delimiter + String.valueOf(key).replaceAll("\\s", "");
        }

        public String getCacheName() {
            return cacheName;
        }
    }
}
