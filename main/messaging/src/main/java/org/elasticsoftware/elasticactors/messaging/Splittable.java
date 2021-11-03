package org.elasticsoftware.elasticactors.messaging;

import com.google.common.collect.ImmutableMap;

import java.util.function.Function;

public interface Splittable<T, D> {

    ImmutableMap<Integer, D> splitInBuckets(Function<T, Integer> hashFunction, int buckets);
}
