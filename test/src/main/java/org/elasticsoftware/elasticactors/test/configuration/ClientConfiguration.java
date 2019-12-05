package org.elasticsoftware.elasticactors.test.configuration;

import org.elasticsoftware.elasticactors.ActorSystemConfiguration;
import org.elasticsoftware.elasticactors.client.RemoteActorSystem;
import org.elasticsoftware.elasticactors.client.SerializationFrameworkCache;
import org.elasticsoftware.elasticactors.messaging.MessageQueueFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;

public class ClientConfiguration {

    @Bean
    public RemoteActorSystem clientActorSystem(
            ActorSystemConfiguration configuration,
            ApplicationContext applicationContext,
            MessageQueueFactory localMessageQueueFactory) {
        return new RemoteActorSystem(
                "testcluster",
                configuration,
                new SerializationFrameworkCache(applicationContext::getBean),
                localMessageQueueFactory);
    }

}
