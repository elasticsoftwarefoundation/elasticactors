package org.elasticsoftware.elasticactors.cluster;

import org.elasticsoftware.elasticactors.ActorContainer;
import org.elasticsoftware.elasticactors.ActorShard;
import org.elasticsoftware.elasticactors.PhysicalNode;

import javax.annotation.Nullable;
import java.util.concurrent.TimeUnit;

/**
 * @author Joost van de Wijgerd
 */
public interface ShardDistributionStrategy {
    /**
     * Signal to the cluster that a Local ActorShard has been given up
     *
     * @param localShard
     * @param nextOwner
     */
    public void signalRelease(ActorShard localShard,PhysicalNode nextOwner) throws Exception;
    /**
     * Wait for signal from the current shard owner, when the signal comes in {@link org.elasticsoftware.elasticactors.ActorContainer#init()}
     * should be called within the implementation
     *
     * @param localShard
     * @param currentOwner
     */
    public void registerWaitForRelease(ActorShard localShard,PhysicalNode currentOwner) throws Exception;
    /**
     *  Block until all Shard that were registered to wait for release are handled, or the specified
     *  waitTime has run out.
     *
     * @param waitTime
     * @param unit
     */
    public boolean waitForReleasedShards(long waitTime,TimeUnit unit);
}
