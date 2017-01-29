/*
 * Copyright 2013 - 2017 The Original Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.elasticsoftware.elasticactors.cluster;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.elasticsoftware.elasticactors.PhysicalNode;

import java.util.List;

/**
 * @author Joost van de Wijgerd
 */
public final class HashingNodeSelector implements NodeSelector {
    private static final int REPLICAS = 128;
    private final ConsistentHash<PhysicalNode> consistentHash;
    private final List<PhysicalNode> allNodes;

    public HashingNodeSelector(List<PhysicalNode> nodes) {
        final HashFunction hashFunction = Hashing.murmur3_128();
        consistentHash = new ConsistentHash<>(hashFunction,REPLICAS,nodes);
        allNodes = nodes;
    }

    @Override
    public List<PhysicalNode> getAll() {
        return allNodes;
    }

    @Override
    public PhysicalNode getPrimary(String shardKey) {
        return consistentHash.get(shardKey);
    }
}
