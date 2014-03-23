package org.elasticsoftware.elasticactors.cluster.scheduler;

import org.elasticsoftware.elasticactors.ActorContainer;
import org.elasticsoftware.elasticactors.ActorContainerRef;
import org.elasticsoftware.elasticactors.scheduler.ScheduledMessageRef;

import static java.lang.String.format;

/**
 * @author Joost van de Wijgerd
 */
public final class DisconnectedRemoteScheduledMessageRef implements ScheduledMessageRef, ActorContainerRef {
    private final String clusterName;
    private final String actorSystemName;
    private final int shardId;
    private final ScheduledMessageKey scheduledMessageKey;

    public DisconnectedRemoteScheduledMessageRef(String clusterName, String actorSystemName, int shardId, ScheduledMessageKey scheduledMessageKey) {
        this.clusterName = clusterName;
        this.actorSystemName = actorSystemName;
        this.shardId = shardId;
        this.scheduledMessageKey = scheduledMessageKey;
    }

    @Override
    public void cancel() {
        throw new IllegalStateException(format("Remote Actor Cluster %s is not configured, ensure a correct remote configuration in the config.yaml",clusterName));
    }

    @Override
    public ActorContainer get() {
        throw new IllegalStateException(format("Remote Actor Cluster %s is not configured, ensure a correct remote configuration in the config.yaml",clusterName));
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
        return format(REFSPEC_FORMAT,clusterName,actorSystemName,shardId,scheduledMessageKey.getFireTime(),scheduledMessageKey.getId().toString());
    }
}
