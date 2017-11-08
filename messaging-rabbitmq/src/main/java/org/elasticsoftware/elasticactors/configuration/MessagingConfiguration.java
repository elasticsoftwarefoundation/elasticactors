/*
 * Copyright 2013 - 2017 The Original Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.elasticsoftware.elasticactors.configuration;

import org.elasticsoftware.elasticactors.cluster.ActorRefFactory;
import org.elasticsoftware.elasticactors.cluster.InternalActorSystem;
import org.elasticsoftware.elasticactors.messaging.MessageQueueFactory;
import org.elasticsoftware.elasticactors.messaging.MessageQueueFactoryFactory;
import org.elasticsoftware.elasticactors.messaging.MessagingService;
import org.elasticsoftware.elasticactors.rabbitmq.RabbitMQMessagingServiceInterface;
import org.elasticsoftware.elasticactors.rabbitmq.MessageAcker;
import org.elasticsoftware.elasticactors.rabbitmq.RabbitMQMessagingService;
import org.elasticsoftware.elasticactors.rabbitmq.health.RabbitMQHealthCheck;
import org.elasticsoftware.elasticactors.serialization.internal.ActorRefDeserializer;
import org.elasticsoftware.elasticactors.serialization.internal.InternalMessageDeserializer;
import org.elasticsoftware.elasticactors.util.concurrent.ThreadBoundExecutor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;

import javax.annotation.PostConstruct;

import static org.elasticsoftware.elasticactors.rabbitmq.MessageAcker.Type.DIRECT;

/**
 * @author Joost van de Wijgerd
 */
public class MessagingConfiguration {
    @Autowired
    private Environment env;
    @Autowired @Qualifier("queueExecutor")
    private ThreadBoundExecutor queueExecutor;
    @Autowired
    private ActorRefFactory actorRefFactory;
    @Autowired
    private InternalActorSystem internalActorSystem;
    private RabbitMQMessagingServiceInterface messagingService;

    @PostConstruct
    public void initialize() {
        String clusterName = env.getRequiredProperty("ea.cluster");
        String rabbitMQHosts = env.getRequiredProperty("ea.rabbitmq.hosts");
        Integer rabbitmqPort = env.getProperty("ea.rabbitmq.port", Integer.class, 5672);
        String rabbitMQUsername= env.getProperty("ea.rabbitmq.username","guest");
        String rabbitMQPassword = env.getProperty("ea.rabbitmq.password","guest");
        MessageAcker.Type ackType = env.getProperty("ea.rabbitmq.ack",MessageAcker.Type.class, DIRECT);
        String threadModel = env.getProperty("ea.rabbitmq.threadmodel", "sc");
        Integer prefetchCount = env.getProperty("ea.rabbitmq.prefetchCount", Integer.class, 0);
        Integer consumerThreadCount = env.getProperty("ea.rabbitmq.consumerThreadCount", Integer.class, Runtime.getRuntime().availableProcessors() * 2);
        if("cpt".equals(threadModel)) {
            messagingService = new org.elasticsoftware.elasticactors.rabbitmq.cpt.RabbitMQMessagingService(clusterName,
                    rabbitMQHosts,
                    rabbitmqPort,
                    rabbitMQUsername,
                    rabbitMQPassword,
                    ackType,
                    queueExecutor,
                    new InternalMessageDeserializer(new ActorRefDeserializer(actorRefFactory), internalActorSystem),
                    prefetchCount,
                    consumerThreadCount);
        } else {
            messagingService = new RabbitMQMessagingService(clusterName,
                    rabbitMQHosts,
                    rabbitmqPort,
                    rabbitMQUsername,
                    rabbitMQPassword,
                    ackType,
                    queueExecutor,
                    new InternalMessageDeserializer(new ActorRefDeserializer(actorRefFactory), internalActorSystem),
                    prefetchCount,
                    consumerThreadCount);
        }
    }

    @Bean(name = {"messagingService,remoteActorSystemMessageQueueFactoryFactory"})
    public MessagingService getMessagingService() {
        return messagingService;
    }

    @Bean(name = {"localMessageQueueFactory"})
    public MessageQueueFactory getLocalMessageQueueFactory() {
        return messagingService.getLocalMessageQueueFactory();
    }

    @Bean(name = {"remoteMessageQueueFactory"})
    public MessageQueueFactory getRemoteMessageQueueFactory() {
        return messagingService.getRemoteMessageQueueFactory();
    }

    @Bean(name = "remoteActorSystemMessageQueueFactoryFactory")
    public MessageQueueFactoryFactory getRemoteActorSystemMessageQueueFactoryFactory() {
        return messagingService.getRemoteActorSystemMessageQueueFactoryFactory();
    }

    @Bean(name = "rabbitMQHealthCheck")
    public RabbitMQHealthCheck getHealthCheck() {
        return new RabbitMQHealthCheck(messagingService);
    }
}
