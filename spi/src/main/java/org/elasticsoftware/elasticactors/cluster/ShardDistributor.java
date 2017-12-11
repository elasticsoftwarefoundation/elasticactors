package org.elasticsoftware.elasticactors.cluster;

import org.elasticsoftware.elasticactors.PhysicalNode;

import java.util.List;

public interface ShardDistributor {
    void updateNodes(List<PhysicalNode> nodes) throws Exception;

    void distributeShards(List<PhysicalNode> nodes, ShardDistributionStrategy strategy) throws Exception;
}
