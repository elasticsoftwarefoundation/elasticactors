package org.elasticsoftware.elasticactors.messaging;

import com.google.common.collect.ImmutableMap;

public interface Splittable<T, D> {

    ImmutableMap<Integer, D> splitInBuckets(Hasher hasher, int buckets);
}
