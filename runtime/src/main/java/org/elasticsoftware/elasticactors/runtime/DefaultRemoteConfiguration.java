package org.elasticsoftware.elasticactors.runtime;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.elasticsoftware.elasticactors.RemoteActorSystemConfiguration;

/**
 * @author Joost van de Wijgerd
 */
public final class DefaultRemoteConfiguration implements RemoteActorSystemConfiguration {
    private final String clusterName;
    private final String name;
    private final int numberOfShards;

    @JsonCreator
    public DefaultRemoteConfiguration(@JsonProperty("clusterName") String clusterName,
                                      @JsonProperty("name") String name,
                                      @JsonProperty("shards") int numberOfShards) {
        this.clusterName = clusterName;
        this.name = name;
        this.numberOfShards = numberOfShards;
    }

    @Override
    public String getName() {
        return name;
    }

    @JsonProperty("shards")
    @Override
    public int getNumberOfShards() {
        return numberOfShards;
    }

    @Override
    public String getClusterName() {
        return clusterName;
    }
}
