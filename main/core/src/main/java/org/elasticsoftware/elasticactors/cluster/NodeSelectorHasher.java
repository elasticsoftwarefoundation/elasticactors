package org.elasticsoftware.elasticactors.cluster;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.elasticsoftware.elasticactors.messaging.Hasher;

import java.nio.charset.StandardCharsets;

public final class NodeSelectorHasher implements Hasher {

    private final HashFunction hashFunction;

    public NodeSelectorHasher() {
        this(53);
    }

    public NodeSelectorHasher(Integer seed) {
        this.hashFunction = seed == null || seed == 0
            ? Hashing.murmur3_128()
            : Hashing.murmur3_128(seed);
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
