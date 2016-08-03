package org.elasticsoftware.elasticactors.cluster;

import org.elasticsoftware.elasticactors.ActorShard;

/**
 * @author Joost van de Wijgerd
 */
public interface ShardAccessor {
    /**
     * Return the {@link org.elasticsoftware.elasticactors.ActorShard} that belongs to the given path.
     *
     * @param actorPath
     * @return
     */
    ActorShard getShard(String actorPath);

    /**
     * Return an ActorShard with the given shardId
     *
     * @param shardId
     * @return
     */
    ActorShard getShard(int shardId);

    int getNumberOfShards();
}
