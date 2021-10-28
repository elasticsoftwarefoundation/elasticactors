/*
 *   Copyright 2013 - 2019 The Original Authors
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.elasticsoftware.elasticactors.cache;

import com.google.common.cache.AbstractCache;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheStats;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;

/**
 * @author Joost van de Wijgerd
 */
public class CacheManager<K,V> {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    private final Cache<CacheKey,V> backingCache;
    private final Multimap<Object,CacheKey> segmentIndex;
    private final ConcurrentMap<Object,EvictionListener<V>> evictionListeners = new ConcurrentHashMap<>();

    public CacheManager(int maximumSize) {
        backingCache = CacheBuilder.newBuilder()
            .maximumSize(maximumSize)
            .removalListener(new GlobalRemovalListener())
            .build();
        segmentIndex = Multimaps.synchronizedSetMultimap(HashMultimap.create());
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
        public V getIfPresent(Object key) {
            return backingCache.getIfPresent(new CacheKey(segmentKey,key));
        }

        @Override
        public V get(K key, Callable<? extends V> valueLoader) throws ExecutionException {
            CacheKey cacheKey = new CacheKey(segmentKey,key);
            V value = backingCache.get(cacheKey,valueLoader);
            segmentIndex.put(segmentKey,cacheKey);
            return value;
        }

        @Override
        public void invalidate(Object key) {
            CacheKey cacheKey = new CacheKey(segmentKey,key);
            backingCache.invalidate(new CacheKey(segmentKey,key));
            segmentIndex.remove(segmentKey,cacheKey);
        }

        @Override
        public void put(K key, V value) {
            CacheKey cacheKey = new CacheKey(segmentKey,key);
            backingCache.put(cacheKey,value);
            segmentIndex.put(segmentKey,cacheKey);
        }

        @Override
        public ImmutableMap<K, V> getAllPresent(Iterable<?> keys) {
            Map<K,V> result = Maps.newLinkedHashMap();
            for (Object key : keys) {
                V value = backingCache.getIfPresent(new CacheKey(segmentKey,key));
                if(value != null) {
                    result.put((K) key, value);
                }
            }
            return ImmutableMap.copyOf(result);
        }

        @Override
        public void putAll(Map<? extends K, ? extends V> m) {
            for (Map.Entry<? extends K, ? extends V> entry : m.entrySet()) {
                put(entry.getKey(),entry.getValue());
            }
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

        @Override
        public CacheStats stats() {
            return backingCache.stats();
        }
    }

    private static final class CacheKey {
        private final Object segmentKey;
        private final Object cacheKey;
        private final int hashCode;

        private CacheKey(Object segmentKey, Object cacheKey) {
            this.segmentKey = segmentKey;
            this.cacheKey = cacheKey;
            this.hashCode = (segmentKey.hashCode() * 31) + cacheKey.hashCode();
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
            if(notification.getKey() != null && notification.wasEvicted()) {
                segmentIndex.remove(notification.getKey().segmentKey,notification.getKey());
            }
            EvictionListener<V> evictionListener = evictionListeners.get(notification.getKey().segmentKey);
            // only notify when it was not evicted explicitly (when a entry was deleted)
            // otherwise the prePassivate will run
            if (logger.isDebugEnabled()) {
                logger.debug(
                    "Removing [{}] from cache. Cause: [{}]",
                    notification.getValue(),
                    notification.getCause()
                );
            }
            if(evictionListener != null && notification.wasEvicted()) {
                evictionListener.onEvicted(notification.getValue());
            }
        }
    }
}
