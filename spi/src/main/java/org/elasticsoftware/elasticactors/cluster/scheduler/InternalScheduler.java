package org.elasticsoftware.elasticactors.cluster.scheduler;

import org.elasticsoftware.elasticactors.ShardKey;

/**
 * @author Joost van de Wijgerd
 */
public interface InternalScheduler {
    public void cancel(ShardKey shardKey,ScheduledMessageKey messageKey);
}
