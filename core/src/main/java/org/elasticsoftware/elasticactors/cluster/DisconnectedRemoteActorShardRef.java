package org.elasticsoftware.elasticactors.cluster;

import org.elasticsoftware.elasticactors.ActorContainer;
import org.elasticsoftware.elasticactors.ActorContainerRef;
import org.elasticsoftware.elasticactors.ActorRef;

import static java.lang.String.format;

/**
 * @author Joost van de Wijgerd
 */
public final class DisconnectedRemoteActorShardRef implements ActorRef,ActorContainerRef {
    public static final String REFSPEC_FORMAT = "actor://%s/%s/shards/%d/%s";
    private final String clusterName;
    private final String actorSystemName;
    private final String actorId;
    private final int shardId;

    public DisconnectedRemoteActorShardRef(String clusterName, String actorSystemName, String actorId, int shardId) {
        this.clusterName = clusterName;
        this.actorSystemName = actorSystemName;
        this.actorId = actorId;
        this.shardId = shardId;
    }


    @Override
    public String getActorPath() {
        return format("%s/shards/%d", actorSystemName, shardId);
    }

    @Override
    public String getActorId() {
        return actorId;
    }

    @Override
    public void tell(Object message, ActorRef sender) {
        tell(message);
    }

    @Override
    public void tell(Object message) throws IllegalStateException {
        throw new IllegalStateException(format("Remote Actor Cluster %s is not configured, ensure a correct remote configuration in the config.yaml",clusterName));
    }

    @Override
    public ActorContainer get() {
        throw new IllegalStateException(format("Remote Actor Cluster %s is not configured, ensure a correct remote configuration in the config.yaml",clusterName));
    }

    @Override
    public String toString() {
        return format(REFSPEC_FORMAT,clusterName,actorSystemName,shardId,actorId);
    }

    @Override
    public boolean equals(Object o) {
        return this == o || o instanceof ActorRef && this.toString().equals(o.toString());
    }

    @Override
    public int hashCode() {
        return toString().hashCode();
    }
}
