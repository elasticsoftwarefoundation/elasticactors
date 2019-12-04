package org.elasticsoftware.elasticactors.test.configuration;

import org.elasticsoftware.elasticactors.ActorSystemConfiguration;
import org.elasticsoftware.elasticactors.client.ClientActorSystem;
import org.elasticsoftware.elasticactors.client.SerializationFrameworkCache;
import org.elasticsoftware.elasticactors.messaging.MessageQueueFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;

public class ClientConfiguration {

    @Bean
    public ClientActorSystem clientActorSystem(
            ActorSystemConfiguration configuration,
            ApplicationContext applicationContext,
            MessageQueueFactory localMessageQueueFactory) {
        return new ClientActorSystem(
                "testcluster",
                configuration,
                new SerializationFrameworkCache(applicationContext::getBean),
                localMessageQueueFactory);
    }

}
