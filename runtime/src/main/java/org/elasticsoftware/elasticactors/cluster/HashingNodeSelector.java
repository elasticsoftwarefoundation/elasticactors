package org.elasticsoftware.elasticactors.cluster;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.elasticsoftware.elasticactors.PhysicalNode;

import java.util.List;

/**
 * @author Joost van de Wijgerd
 */
public final class HashingNodeSelector implements NodeSelector {
    private static final int SEED = 156788472;
    private static final int REPLICAS = 256;
    private final ConsistentHash<PhysicalNode> consistentHash;
    private final List<PhysicalNode> allNodes;

    public HashingNodeSelector(List<PhysicalNode> nodes) {
        final HashFunction hashFunction = Hashing.murmur3_128(SEED);
        consistentHash = new ConsistentHash<PhysicalNode>(hashFunction,REPLICAS,nodes);
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
