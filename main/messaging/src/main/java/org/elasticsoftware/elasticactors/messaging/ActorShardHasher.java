/*
 * Copyright 2013 - 2025 The Original Authors
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
