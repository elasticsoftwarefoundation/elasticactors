package org.elasticsoftware.elasticactors.messaging;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import java.nio.charset.StandardCharsets;

public final class ActorShardHasher implements Hasher {

    private final HashFunction hashFunction;

    // Using 0 for backwards compatibility, but using a prime number would be better
    public ActorShardHasher() {
        this(0);
    }

    public ActorShardHasher(Integer seed) {
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
