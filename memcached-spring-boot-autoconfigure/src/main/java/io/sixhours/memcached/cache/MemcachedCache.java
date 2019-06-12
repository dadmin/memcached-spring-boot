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

    private static final String KEY_DELIMITER = ":";

    private final MemcachedClient memcachedClient;
    private final MemcacheCacheMetadata memcacheCacheMetadata;

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
     * @param namespace       Cache invalidation namespace key
     */
    public MemcachedCache(String name, MemcachedClient memcachedClient, int expiration, String prefix, String namespace) {
        super(true);
        this.memcachedClient = memcachedClient;
        this.memcacheCacheMetadata = new MemcacheCacheMetadata(name, expiration, prefix, namespace);
    }

    @Override
    protected Object lookup(Object key) {
        if(key instanceof Collection){
            Map<String, String> keyToMemcachedKey = (Map<String, String>) ((Collection) key).stream().collect(Collectors.toMap(k -> k, this::memcachedKey));
            Map<String, Object> values = memcachedClient.getBulk(keyToMemcachedKey.values());
            trackCollectionHitsMisses(values.size(), ((Collection) key).size() - values.size());
            //replace memcached keys with origin keys in result
            keyToMemcachedKey.entrySet().forEach(entry -> {
                Object value = values.remove(entry.getValue());
                values.put(entry.getKey(), value);
            });
            return values;
        }
        return trackHitsMisses(memcachedClient.get(memcachedKey(key)));
    }

    @Override
    public String getName() {
        return this.memcacheCacheMetadata.name();
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
        this.memcachedClient.set(memcachedKey(key), this.memcacheCacheMetadata.expiration(), toStoreValue(value));
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
        this.memcachedClient.delete(memcachedKey(key));
    }

    @Override
    public void clear() {
        memcacheCacheMetadata.incrNamespaceValue();
        memcachedClient.set(memcacheCacheMetadata.namespaceKey(), memcacheCacheMetadata.expiration(), memcacheCacheMetadata.namespaceValue());
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
     * @param hitsCount how much keys were presented in cache
     * @param missesCount how much keys weren't presented in cache
     */
    private void trackCollectionHitsMisses(int hitsCount, int missesCount) {
        hits.addAndGet(hitsCount);
        misses.addAndGet(missesCount);
    }

    /**
     * Gets Memcached key value.
     * <p>
     * Prepends cache prefix and namespace value to the given {@code key}. All whitespace characters will be stripped from
     * the {@code key} value, for Memcached key to be valid.
     *
     * @param key The key
     * @return Memcached key
     */
    private String memcachedKey(Object key) {
        return memcacheCacheMetadata.keyPrefix() +
                memcacheCacheMetadata.namespaceValue() +
                KEY_DELIMITER +
                String.valueOf(key).replaceAll("\\s", "");
    }

    /**
     * Gets namespace value from the cache. The value is used for invalidation of the cache data
     * by incrementing current namespace value by 1.
     *
     * @param namespaceKey namespace key
     * @param expiration cache ttl
     * @return Namespace integer value returned as {@code String}
     */
    private String namespaceValue(String namespaceKey, int expiration) {
        String value = (String) this.memcachedClient.get(namespaceKey);
        if (value == null) {
            value = String.valueOf(System.currentTimeMillis());
            this.memcachedClient.set(namespaceKey, expiration, value);
        }

        return value;
    }

    class MemcacheCacheMetadata {
        private final String name;
        private final int expiration;
        private final String keyPrefix;
        private final String namespaceKey;
        private String namespaceValue;

        public MemcacheCacheMetadata(String name, int expiration, String cachePrefix, String namespace) {
            this.name = name;
            this.expiration = expiration;

            StringBuilder sb = new StringBuilder(cachePrefix)
                    .append(KEY_DELIMITER)
                    .append(name)
                    .append(KEY_DELIMITER);

            this.keyPrefix = sb.toString();
            this.namespaceKey = sb.append(namespace).toString();
            this.namespaceValue = MemcachedCache.this.namespaceValue(namespaceKey, expiration);
        }

        public String name() {
            return name;
        }

        public int expiration() {
            return expiration;
        }

        public String keyPrefix() {
            return keyPrefix;
        }

        public String namespaceKey() {
            return namespaceKey;
        }

        public String namespaceValue() {
            return namespaceValue;
        }

        public void incrNamespaceValue(){
            namespaceValue = String.valueOf(Long.valueOf(namespaceValue) + 1);
        }
    }
}
