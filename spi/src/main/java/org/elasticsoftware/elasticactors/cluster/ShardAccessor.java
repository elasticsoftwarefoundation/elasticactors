package org.elasticsoftware.elasticactors.cluster;

import org.elasticsoftware.elasticactors.ActorShard;

/**
 * @author Joost van de Wijgerd
 */
public interface ShardAccessor {
    /**
     * Return the {@link org.elasticsoftware.elasticactors.ActorShard} that belongs to the give path.
     *
     * @param actorPath
     * @return
     */
    ActorShard getShard(String actorPath);

    ActorShard getShard(int shardId);
}
