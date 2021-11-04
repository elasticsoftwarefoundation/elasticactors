package org.elasticsoftware.elasticactors.messaging;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import java.nio.charset.StandardCharsets;

public final class MultiMessageQueueProxyHasher implements Hasher {

    private final HashFunction hashFunction;

    // Using a good default prime seed because the 0 has a strong bias towards even numbers
    public MultiMessageQueueProxyHasher() {
        this(53);
    }

    public MultiMessageQueueProxyHasher(Integer seed) {
        this.hashFunction = seed == null || seed == 0
            ? Hashing.murmur3_32()
            : Hashing.murmur3_32(seed);
    }

    @Override
    public int hashStringToInt(String value) {
        return hashFunction.hashString(value, StandardCharsets.UTF_8).asInt();
    }

    @Override
    public long hashStringToLong(String value) {
        return hashFunction.hashString(value, StandardCharsets.UTF_8).asLong();
    }
}
