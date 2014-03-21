package org.elasticsoftware.elasticactors.cluster;

import org.elasticsoftware.elasticactors.ActorRef;

import static java.lang.String.format;

/**
 * @author Joost van de Wijgerd
 */
public final class DisconnectedRemoteActorShardRef implements ActorRef {
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
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DisconnectedRemoteActorShardRef that = (DisconnectedRemoteActorShardRef) o;

        return actorId.equals(that.actorId) && actorSystemName.equals(that.actorSystemName) && shardId == that.shardId && clusterName.equals(that.clusterName);

    }

    @Override
    public int hashCode() {
        int result = clusterName.hashCode();
        result = 31 * result + actorSystemName.hashCode();
        result = 31 * result + actorId.hashCode();
        result = 31 * result + shardId;
        return result;
    }
}
