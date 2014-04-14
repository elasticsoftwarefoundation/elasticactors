package org.elasticsoftware.elasticactors.cluster.strategies;

import org.elasticsoftware.elasticactors.ActorShard;
import org.elasticsoftware.elasticactors.PhysicalNode;
import org.elasticsoftware.elasticactors.cluster.ShardDistributionStrategy;

import java.util.concurrent.TimeUnit;

/**
 * @author Joost van de Wijgerd
 */
public final class RunningNodeScaleDownStrategy implements ShardDistributionStrategy {
    @Override
    public void signalRelease(ActorShard localShard, PhysicalNode nextOwner) {
        // won't happen. the cluster is scaling down so we'll be taking over shards only
    }

    @Override
    public void registerWaitForRelease(ActorShard localShard, PhysicalNode currentOwner) throws Exception {
        // other node possibly crashed, otherwise shutting down
        // @todo: maybe make a distinction between crash and controlled shutdown here
        localShard.init();
    }

    @Override
    public boolean waitForReleasedShards(long waitTime, TimeUnit unit) {
        return true;
    }
}
