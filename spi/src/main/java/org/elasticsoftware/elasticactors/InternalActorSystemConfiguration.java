package org.elasticsoftware.elasticactors;

import org.elasticsoftware.elasticactors.serialization.MessageDeliveryMode;

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

    /**
     * Returns the default {@link org.elasticsoftware.elasticactors.serialization.MessageDeliveryMode} that is used when
     * {@link org.elasticsoftware.elasticactors.serialization.Message#deliveryMode()} equals {@link org.elasticsoftware.elasticactors.serialization.MessageDeliveryMode#SYSTEM_DEFAULT}
     *
     *
     * @return
     */
    MessageDeliveryMode getMessageDeliveryMode();
}
