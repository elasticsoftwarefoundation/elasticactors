package org.elasticsoftware.elasticactors;

import java.util.List;
import java.util.Set;

/**
 * @author Joost van de Wijgerd
 */
public interface InternalActorSystemConfiguration extends ActorSystemConfiguration {
    /**
     * Return the singleton service instance
     *
     * @param serviceId
     * @return
     */
    ElasticActor<?> getService(String serviceId);

    /**
     * return a list of service actor id's
     *
     * @return
     */
    Set<String> getServices();

    /**
     * Return the remote configurations
     *
     * @return
     */
    List<? extends RemoteActorSystemConfiguration> getRemoteConfigurations();
}
