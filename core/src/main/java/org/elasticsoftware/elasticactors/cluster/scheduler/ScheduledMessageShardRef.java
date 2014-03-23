package org.elasticsoftware.elasticactors.cluster.scheduler;

import org.elasticsoftware.elasticactors.*;
import org.elasticsoftware.elasticactors.messaging.internal.CancelScheduledMessageMessage;
import org.elasticsoftware.elasticactors.scheduler.ScheduledMessageRef;

import static java.lang.String.format;

/**
 * @author Joost van de Wijgerd
 */
public final class ScheduledMessageShardRef implements ScheduledMessageRef, ActorContainerRef {
    private final String clusterName;
    private final ActorShard shard;
    private final ScheduledMessageKey scheduledMessageKey;

    public ScheduledMessageShardRef(String clusterName, ActorShard shard, ScheduledMessageKey scheduledMessageKey) {
        this.clusterName = clusterName;
        this.shard = shard;
        this.scheduledMessageKey = scheduledMessageKey;
    }

    @Override
    public ActorContainer get() {
        return shard;
    }

    @Override
    public void cancel() throws Exception {
        // try to determine the sender if possible
        final ActorRef sender = ActorContextHolder.getSelf();
        final CancelScheduledMessageMessage cancelMessage = new CancelScheduledMessageMessage(scheduledMessageKey.getId(),scheduledMessageKey.getFireTime());
        this.shard.sendMessage(sender,shard.getActorRef(),cancelMessage);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if(!(o instanceof ScheduledMessageRef)) return false;
        ScheduledMessageRef that = (ScheduledMessageRef) o;
        return that.toString().equals(this.toString());
    }

    @Override
    public int hashCode() {
        return toString().hashCode();
    }

    @Override
    public String toString() {
        return format(REFSPEC_FORMAT,clusterName,shard.getKey().getActorSystemName(),shard.getKey().getShardId(),scheduledMessageKey.getFireTime(),scheduledMessageKey.getId().toString());
    }
}
