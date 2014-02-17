package org.elasticsoftware.elasticactors;

/**
 * @author Joost van de Wijgerd
 */
public interface RemoteActorSystemConfiguration {
    String getName();

    int getNumberOfShards();

    String getClusterName();
}
