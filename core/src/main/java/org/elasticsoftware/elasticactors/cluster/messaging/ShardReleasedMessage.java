package org.elasticsoftware.elasticactors.cluster.messaging;

import java.io.Serializable;

/**
 * @author Joost van de Wijgerd
 */
public final class ShardReleasedMessage implements Serializable {
    private final String actorSystem;
    private final int shardId;

    public ShardReleasedMessage(String actorSystem, int shardId) {
        this.actorSystem = actorSystem;
        this.shardId = shardId;
    }

    public String getActorSystem() {
        return actorSystem;
    }

    public int getShardId() {
        return shardId;
    }
}
