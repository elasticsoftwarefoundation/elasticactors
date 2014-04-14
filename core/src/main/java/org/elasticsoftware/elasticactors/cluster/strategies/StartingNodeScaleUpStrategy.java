package org.elasticsoftware.elasticactors.cluster.strategies;

import org.elasticsoftware.elasticactors.ActorShard;
import org.elasticsoftware.elasticactors.PhysicalNode;
import org.elasticsoftware.elasticactors.cluster.messaging.ShardReleasedMessage;

import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author Joost van de Wijgerd
 */
public final class StartingNodeScaleUpStrategy extends MultiNodeScaleUpStrategy {

    public StartingNodeScaleUpStrategy(LinkedBlockingQueue<ShardReleasedMessage> shardReleasedMessages) {
        super(shardReleasedMessages);
    }

    @Override
    public void signalRelease(ActorShard localShard, PhysicalNode nextOwner) {
        // we don't own any shards on startup
    }

}
