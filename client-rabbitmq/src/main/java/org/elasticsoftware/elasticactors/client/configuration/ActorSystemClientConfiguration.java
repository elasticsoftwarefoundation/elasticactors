package org.elasticsoftware.elasticactors.client.configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsoftware.elasticactors.base.serialization.ObjectMapperBuilder;
import org.elasticsoftware.elasticactors.client.rabbitmq.RabbitMQActorSystemClient;
import org.elasticsoftware.elasticactors.cluster.ActorRefFactory;
import org.elasticsoftware.elasticactors.runtime.MessagesScanner;
import org.elasticsoftware.elasticactors.serialization.SystemSerializationFramework;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;

public class ActorSystemClientConfiguration {
    @Autowired
    private Environment env;

    @Bean(name = {"objectMapper"})
    public ObjectMapper createObjectMapper(ActorRefFactory actorRefFactory) {
        String basePackages = env.getProperty("ea.scan.packages",String.class,"");
        Boolean useAfterburner = env.getProperty("ea.base.useAfterburner",Boolean.class,Boolean.TRUE);
        // @todo: fix version
        ObjectMapperBuilder builder = new ObjectMapperBuilder(actorRefFactory, refSpec -> null, "1.0.0", basePackages);
        builder.setUseAfterBurner(useAfterburner);
        return builder.build();
    }

    @Bean(name = "actorSystemClient")
    public RabbitMQActorSystemClient createActorSystemClient(ListableBeanFactory beanFactory) {
        String clusterName = env.getRequiredProperty("ea.cluster");
        String rabbitMQHosts = env.getRequiredProperty("ea.rabbitmq.hosts");
        Integer rabbitmqPort = env.getProperty("ea.rabbitmq.port", Integer.class, 5672);
        String rabbitMQUsername= env.getProperty("ea.rabbitmq.username","guest");
        String rabbitMQPassword = env.getProperty("ea.rabbitmq.password","guest");
        String nodeId = env.getRequiredProperty("ea.node.id");
        int maximumSize = env.getProperty("ea.actorRefCache.maximumSize",Integer.class,10240);
        int numberOfShards = env.getRequiredProperty("ea.cluster.shards", Integer.class);
        String actorSystemName = env.getRequiredProperty("ea.cluster.actorSystemName");
        return new RabbitMQActorSystemClient(clusterName, actorSystemName, numberOfShards, nodeId, rabbitMQHosts, rabbitMQUsername, rabbitMQPassword, beanFactory, maximumSize);
    }


    @Bean(name = "systemSerializationFramework")
    public SystemSerializationFramework createSystemSerializationFramework(RabbitMQActorSystemClient actorSystemClient) {
        return new SystemSerializationFramework(actorSystemClient, actorSystemClient, actorSystemClient);
    }

    @Bean(name = {"messagesScanner"})
    public MessagesScanner createMessageScanner() {
        return new MessagesScanner();
    }
}
