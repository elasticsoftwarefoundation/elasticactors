package org.elasticsoftware.elasticactors.health;

import org.elasticsoftware.elasticactors.ActorShard;
import org.elasticsoftware.elasticactors.PhysicalNode;
import org.elasticsoftware.elasticactors.ShardKey;
import org.elasticsoftware.elasticactors.cluster.*;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.PreDestroy;
import java.util.List;

import static org.elasticsoftware.elasticactors.health.HealthCheckResult.healthy;
import static org.elasticsoftware.elasticactors.health.HealthCheckResult.unhealthy;

/**
 * @author Rob de Boer
 */
public class InternalActorSystemHealthCheck implements HealthCheck, ClusterEventListener {

    private final NodeSelectorFactory nodeSelectorFactory;
    private final ClusterService clusterService;
    private final InternalActorSystem internalActorSystem;
    private List<PhysicalNode> currentTopology;

    @Autowired
    public InternalActorSystemHealthCheck(InternalActorSystem internalActorSystem, ClusterService clusterService, NodeSelectorFactory nodeSelectorFactory) {
        this.nodeSelectorFactory = nodeSelectorFactory;
        this.clusterService = clusterService;
        this.internalActorSystem = internalActorSystem;

        this.clusterService.addEventListener(this);
    }

    @PreDestroy
    public void destroy() {
        if (clusterService != null) {
            clusterService.removeEventListener(this);
        }
    }

    @Override
    public HealthCheckResult check() {
        if (currentTopology == null) {
            return unhealthy("No node topology has been been provided yet for the health check");
        }

        NodeSelector nodeSelector = nodeSelectorFactory.create(currentTopology);

        for (int i = 0; i < internalActorSystem.getNumberOfShards(); i++) {
            ActorShard currentShard = internalActorSystem.getShard(i);
            ShardKey shardKey = new ShardKey(internalActorSystem.getName(), i);
            PhysicalNode node = nodeSelector.getPrimary(shardKey.toString());

            if (currentShard == null) {
                return unhealthy(String.format("Shard %d has not been created yet", i));
            } else if (node.isLocal() && !currentShard.getOwningNode().isLocal()) {
                return unhealthy(String.format("Shard %d is owned by remote node '%s', while it should have been owned by local node '%s'",
                        i, currentShard.getOwningNode().getId(), node.getId()));
            } else if (!node.isLocal() && currentShard.getOwningNode().isLocal()) {
                return unhealthy(String.format("Shard %d is owned by local node '%s', while it should have been owned by remote node '%s'",
                        i, currentShard.getOwningNode().getId(), node.getId()));
            }
        }

        return healthy();
    }

    @Override
    public void onTopologyChanged(List<PhysicalNode> topology) throws Exception {
        this.currentTopology = topology;
    }

    @Override
    public void onMasterElected(PhysicalNode masterNode) throws Exception {
        // not interested in this
    }
}