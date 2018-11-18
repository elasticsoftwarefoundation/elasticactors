package org.elasticsoftware.elasticactors.client.configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsoftware.elasticactors.base.serialization.ObjectMapperBuilder;
import org.elasticsoftware.elasticactors.cluster.ActorRefFactory;
import org.elasticsoftware.elasticactors.cluster.InternalActorSystems;
import org.elasticsoftware.elasticactors.runtime.MessagesScanner;
import org.elasticsoftware.elasticactors.serialization.SystemSerializationFramework;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;

public class ActorSystemClientConfiguration {
    @Autowired
    private Environment env;

    @Bean(name = {"objectMapper"})
    public ObjectMapper createObjectMapper(ActorRefFactory actorRefFactory) {
        String basePackages = env.getProperty("ea.scan.packages",String.class,"");
        Boolean useAfterburner = env.getProperty("ea.base.useAfterburner",Boolean.class,Boolean.FALSE);
        // @todo: fix version
        ObjectMapperBuilder builder = new ObjectMapperBuilder(actorRefFactory, refSpec -> null, "1.0.0", basePackages);
        builder.setUseAfterBurner(useAfterburner);
        return builder.build();
    }


    @Bean(name = "systemSerializationFramework")
    public SystemSerializationFramework createSystemSerializationFramework(InternalActorSystems internalActorSystems) {
        return new SystemSerializationFramework(internalActorSystems);
    }

    @Bean(name = {"messagesScanner"})
    public MessagesScanner createMessageScanner() {
        return new MessagesScanner();
    }
}
