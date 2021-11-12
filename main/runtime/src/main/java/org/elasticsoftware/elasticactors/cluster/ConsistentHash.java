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

package org.elasticsoftware.elasticactors.cluster;

import org.elasticsoftware.elasticactors.messaging.Hasher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public final class ConsistentHash<T> {

    private final static Logger logger = LoggerFactory.getLogger(ConsistentHash.class);

    private final Hasher hasher;
    private final int numberOfReplicas;
    private final SortedMap<Long, T> circle = new TreeMap<>();
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    public ConsistentHash(
        Hasher hasher,
        int numberOfReplicas,
        Collection<T> nodes)
    {
        this.hasher = hasher;
        this.numberOfReplicas = numberOfReplicas;

        for (T node : nodes) {
            internalAdd(node);
        }
    }

    public void add(T node) {
        Lock writeLock = lock.writeLock();
        try {
            writeLock.lock();
            internalAdd(node);
        } finally {
            writeLock.unlock();
        }
    }

    private void internalAdd(T node) {
        for (int i = 0; i < numberOfReplicas; i++) {
            T previous = circle.put(hasher.hashStringToLong(buildReplicaName(node, i)), node);
            if (previous != null) {
                logger.warn(
                    "Detected collision between {} and {}. Distribution might be affected.",
                    previous,
                    node
                );
            }
        }
    }

    public void remove(T node) {
        Lock writeLock = lock.writeLock();
        try {
            writeLock.lock();
            internalRemove(node);
        } finally {
            writeLock.unlock();
        }
    }

    private void internalRemove(T node) {
        for (int i = 0; i < numberOfReplicas; i++) {
            circle.remove(hasher.hashStringToLong(buildReplicaName(node, i)));
        }
    }

    private String buildReplicaName(T node, int i) {
        return node.toString() + "-" + i;
    }

    public T get(String key) {
        Lock readLock = lock.readLock();
        try {
            readLock.lock();
            if (circle.isEmpty()) {
                return null;
            }
            long hash = hasher.hashStringToLong(key);
            if (!circle.containsKey(hash)) {
                SortedMap<Long, T> tailMap = circle.tailMap(hash);
                hash = tailMap.isEmpty() ? circle.firstKey() : tailMap.firstKey();
            }
            return circle.get(hash);
        } finally {
            readLock.unlock();
        }
    }

}
