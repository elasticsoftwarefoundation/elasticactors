package org.elasticsoftware.elasticactors.messaging;

/**
 * @author Joost van de Wijgerd
 */
public interface MessageQueueFactoryFactory {
    MessageQueueFactory create(String clusterName);
}
