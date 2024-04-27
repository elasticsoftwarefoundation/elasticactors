/*
 * Copyright 2013 - 2024 The Original Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */

package org.elasticsoftware.elasticactors.cache;

import com.google.common.cache.AbstractCache;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheStats;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;
import com.google.common.collect.Multimaps;
import io.micrometer.core.instrument.binder.cache.GuavaCacheMetrics;
import org.elasticsoftware.elasticactors.cluster.metrics.MicrometerConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;

/**
 * @author Joost van de Wijgerd
 */
public class CacheManager<K,V> {

    // Pooling keys for read-only operations
    private final static ThreadLocal<CacheKey> keyPool = ThreadLocal.withInitial(CacheKey::new);

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    private final Cache<CacheKey,V> backingCache;
    private final Multimap<Object,CacheKey> segmentIndex;
    private final ConcurrentMap<Object,EvictionListener<V>> evictionListeners = new ConcurrentHashMap<>();

    public CacheManager(int maximumSize, @Nullable MicrometerConfiguration micrometerConfiguration) {
        CacheBuilder<CacheKey, V> builder = CacheBuilder.newBuilder()
            .maximumSize(maximumSize)
            .removalListener(new GlobalRemovalListener());
        if (micrometerConfiguration != null) {
            builder.recordStats();
            backingCache = GuavaCacheMetrics.monitor(
                micrometerConfiguration.getRegistry(),
                builder.build(),
                micrometerConfiguration.getComponentName(),
                micrometerConfiguration.getTags()
            );
        } else {
            backingCache = builder.build();
        }
        segmentIndex = Multimaps.synchronizedMultimap(MultimapBuilder.hashKeys().hashSetValues().build());
    }

    public final Cache<K,V> create(Object cacheKey,EvictionListener<V> evictionListener) {
        // make sure any leftovers from a previous create / destroy are all gone
        backingCache.invalidateAll(segmentIndex.removeAll(cacheKey));
        if(evictionListener != null) {
            evictionListeners.put(cacheKey,evictionListener);
        }
        return new SegmentedCache(cacheKey);
    }

    public final void destroy(Cache<K,V> cache) {
        if(SegmentedCache.class.isInstance(cache)) {
            Object segmentKey = ((SegmentedCache)cache).segmentKey;
            backingCache.invalidateAll(segmentIndex.removeAll(segmentKey));
            evictionListeners.remove(segmentKey);
        }
    }

    private final class SegmentedCache extends AbstractCache<K,V> {
        private final Object segmentKey;

        private SegmentedCache(Object segmentKey) {
            this.segmentKey = segmentKey;
        }

        @Override
        public V getIfPresent(@Nonnull Object key) {
            return backingCache.getIfPresent(keyPool.get().fill(segmentKey, key));
        }

        @Override
        public V get(@Nonnull K key, @Nonnull Callable<? extends V> valueLoader) throws ExecutionException {
            CacheKey cacheKey = new CacheKey().fill(segmentKey, key);
            V value = backingCache.get(cacheKey,valueLoader);
            segmentIndex.put(segmentKey,cacheKey);
            return value;
        }

        @Override
        public void invalidate(@Nonnull Object key) {
            CacheKey cacheKey = keyPool.get().fill(segmentKey, key);
            backingCache.invalidate(cacheKey);
            segmentIndex.remove(segmentKey,cacheKey);
        }

        @Override
        public void put(@Nonnull K key, @Nonnull V value) {
            CacheKey cacheKey = new CacheKey().fill(segmentKey, key);
            backingCache.put(cacheKey,value);
            segmentIndex.put(segmentKey,cacheKey);
        }

        @Nonnull
        @Override
        public ImmutableMap<K, V> getAllPresent(@Nonnull Iterable<?> keys) {
            ImmutableMap.Builder<K,V> result = ImmutableMap.builder();
            CacheKey pooledKey = keyPool.get();
            for (Object key : keys) {
                V value = backingCache.getIfPresent(pooledKey.fill(segmentKey, key));
                if(value != null) {
                    result.put((K) key, value);
                }
            }
            return result.build();
        }

        @Override
        public void putAll(Map<? extends K, ? extends V> m) {
            m.forEach(this::put);
        }

        @Override
        public void cleanUp() {
            backingCache.cleanUp();
        }

        @Override
        public long size() {
            return segmentIndex.get(segmentKey).size();
        }

        @Override
        public void invalidateAll(Iterable<?> keys) {
            for (Object key : keys) {
                invalidate(key);
            }
        }

        @Override
        public void invalidateAll() {
            backingCache.invalidateAll(segmentIndex.removeAll(segmentKey));
        }

        @Nonnull
        @Override
        public CacheStats stats() {
            return backingCache.stats();
        }

        /**
         * Unlike most implementations, the returned map has no connection to the underlying cache.
         */
        @Override
        public ConcurrentMap<K, V> asMap() {
            // Getting the cache as a map here, so we don't skew the statistics
            Map<CacheKey, V> backingCacheAsMap = backingCache.asMap();
            Collection<CacheKey> cacheKeysForSegment = segmentIndex.get(segmentKey);
            ConcurrentMap<K, V> result = new ConcurrentHashMap<>(cacheKeysForSegment.size());
            for (CacheKey key : cacheKeysForSegment) {
                V value = backingCacheAsMap.get(key);
                if (value != null) {
                    result.put((K) key.cacheKey, value);
                }
            }
            return result;
        }
    }

    private static final class CacheKey {
        private Object segmentKey;
        private Object cacheKey;
        private int hashCode;

        public CacheKey fill(Object segmentKey, Object cacheKey) {
            this.segmentKey = segmentKey;
            this.cacheKey = cacheKey;
            this.hashCode = (segmentKey.hashCode() * 31) + cacheKey.hashCode();
            return this;
        }

        public CacheKey copy() {
            CacheKey copy = new CacheKey();
            copy.segmentKey = this.segmentKey;
            copy.cacheKey = this.cacheKey;
            copy.hashCode = this.hashCode;
            return copy;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof CacheKey)) {
                return false;
            }

            CacheKey cacheKey1 = (CacheKey) o;

            return cacheKey.equals(cacheKey1.cacheKey)
                && segmentKey.equals(cacheKey1.segmentKey);
        }

        @Override
        public int hashCode() {
            return this.hashCode;
        }
    }

    private final class GlobalRemovalListener implements RemovalListener<CacheManager.CacheKey,V> {

        @Override
        public void onRemoval(RemovalNotification<CacheKey, V> notification) {
            if (notification.getKey() != null) {
                if (notification.wasEvicted()) {
                    segmentIndex.remove(notification.getKey().segmentKey, notification.getKey());
                    EvictionListener<V> evictionListener = evictionListeners.get(notification.getKey().segmentKey);
                    // only notify when it was not evicted explicitly (when an entry was deleted)
                    // otherwise the prePassivate will run
                    if (evictionListener != null) {
                        evictionListener.onEvicted(notification.getValue());
                    }
                }
                if (logger.isDebugEnabled()) {
                    logger.debug(
                        "Removing [{}] from cache. Cause: [{}]",
                        notification.getValue(),
                        notification.getCause()
                    );
                }
            }
        }
    }
}
