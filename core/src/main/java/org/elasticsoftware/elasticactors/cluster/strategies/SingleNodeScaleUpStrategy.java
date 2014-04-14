package org.elasticsoftware.elasticactors.cluster.strategies;

import org.elasticsoftware.elasticactors.ActorShard;
import org.elasticsoftware.elasticactors.PhysicalNode;
import org.elasticsoftware.elasticactors.cluster.ShardDistributionStrategy;

import java.util.concurrent.TimeUnit;

/**
 * @author Joost van de Wijgerd
 */
public final class SingleNodeScaleUpStrategy implements ShardDistributionStrategy {
    @Override
    public void signalRelease(ActorShard localShard, PhysicalNode nextOwner) {
        // won't happen. as we are the single node in the cluster
    }

    @Override
    public void registerWaitForRelease(ActorShard localShard, PhysicalNode currentOwner) throws Exception {
        // no need to wait, run init immediately
        localShard.init();
    }

    @Override
    public boolean waitForReleasedShards(long waitTime, TimeUnit unit) {
        return true;
    }
}
