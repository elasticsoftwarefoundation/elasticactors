package org.elasticsoftware.elasticactors.cluster.scheduler;

import org.elasticsoftware.elasticactors.ActorSystem;
import org.elasticsoftware.elasticactors.ShardKey;
import org.elasticsoftware.elasticactors.scheduler.Scheduler;

/**
 * @author Joost van de Wijgerd
 */
public interface SchedulerService extends Scheduler {
    /**
     * Register a local shard with the SchedulerService. The implementation of the service should
     * load the proper resources for the shard
     *
     * @param shardKey
     */
    public void registerShard(ShardKey shardKey);

    /**
     * Unregister a previously registered shard, release all allocated resources as another node is
     * the new owner of this shard
     *
     * @param shardKey
     */
    public void unregisterShard(ShardKey shardKey);
}
