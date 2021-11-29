/*
 *   Copyright 2013 - 2019 The Original Authors
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.elasticsoftware.elasticactors.configuration;

import io.micrometer.core.instrument.MeterRegistry;
import org.elasticsoftware.elasticactors.cluster.ActorRefFactory;
import org.elasticsoftware.elasticactors.cluster.metrics.MeterConfiguration;
import org.elasticsoftware.elasticactors.cluster.metrics.MeterTagCustomizer;
import org.elasticsoftware.elasticactors.messaging.MessageQueueFactory;
import org.elasticsoftware.elasticactors.messaging.MessageQueueFactoryFactory;
import org.elasticsoftware.elasticactors.rabbitmq.MessageAcker;
import org.elasticsoftware.elasticactors.rabbitmq.RabbitMQMessagingService;
import org.elasticsoftware.elasticactors.rabbitmq.cpt.MultiProducerRabbitMQMessagingService;
import org.elasticsoftware.elasticactors.rabbitmq.health.RabbitMQHealthCheck;
import org.elasticsoftware.elasticactors.rabbitmq.sc.SingleProducerRabbitMQMessagingService;
import org.elasticsoftware.elasticactors.serialization.SerializationAccessor;
import org.elasticsoftware.elasticactors.serialization.internal.ActorRefDeserializer;
import org.elasticsoftware.elasticactors.serialization.internal.InternalMessageDeserializer;
import org.elasticsoftware.elasticactors.util.concurrent.ThreadBoundExecutor;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;
import org.springframework.lang.Nullable;

import static org.elasticsoftware.elasticactors.rabbitmq.MessageAcker.Type.DIRECT;

/**
 * @author Joost van de Wijgerd
 */
public class MessagingConfiguration {

    @Bean(name = {"messagingService"})
    public RabbitMQMessagingService getMessagingService(
        @Qualifier("queueExecutor") ThreadBoundExecutor queueExecutor,
        Environment env,
        ActorRefFactory actorRefFactory,
        SerializationAccessor serializationAccessor,
        @Nullable @Qualifier("elasticActorsMeterRegistry") MeterRegistry meterRegistry,
        @Nullable @Qualifier("elasticActorsMeterTagCustomizer") MeterTagCustomizer tagCustomizer)
    {
        String clusterName = env.getRequiredProperty("ea.cluster");
        String rabbitMQHosts = env.getRequiredProperty("ea.rabbitmq.hosts");
        Integer rabbitmqPort = env.getProperty("ea.rabbitmq.port", Integer.class, 5672);
        String rabbitMQUsername = env.getProperty("ea.rabbitmq.username", "guest");
        String rabbitMQPassword = env.getProperty("ea.rabbitmq.password", "guest");
        MessageAcker.Type ackType =
            env.getProperty("ea.rabbitmq.ack", MessageAcker.Type.class, DIRECT);
        String threadModel = env.getProperty("ea.rabbitmq.threadmodel", "sc").toLowerCase().trim();
        Integer prefetchCount = env.getProperty("ea.rabbitmq.prefetchCount", Integer.class, 0);
        MeterConfiguration meterConfiguration =
            MeterConfiguration.build(env, meterRegistry, "rabbitmq", tagCustomizer);
        MeterConfiguration ackerMeterConfiguration =
            MeterConfiguration.build(env, meterRegistry, "rabbitmqAcker", tagCustomizer);
        InternalMessageDeserializer messageDeserializer = new InternalMessageDeserializer(
            new ActorRefDeserializer(actorRefFactory),
            serializationAccessor
        );
        if ("cpt".equals(threadModel)) {
            return new MultiProducerRabbitMQMessagingService(
                clusterName,
                rabbitMQHosts,
                rabbitmqPort,
                rabbitMQUsername,
                rabbitMQPassword,
                ackType,
                queueExecutor,
                messageDeserializer,
                prefetchCount,
                meterConfiguration,
                ackerMeterConfiguration
            );
        } else {
            return new SingleProducerRabbitMQMessagingService(
                clusterName,
                rabbitMQHosts,
                rabbitmqPort,
                rabbitMQUsername,
                rabbitMQPassword,
                ackType,
                queueExecutor,
                messageDeserializer,
                prefetchCount,
                meterConfiguration,
                ackerMeterConfiguration
            );
        }
    }

    @Bean(name = {"localMessageQueueFactory"})
    public MessageQueueFactory getLocalMessageQueueFactory(
        RabbitMQMessagingService messagingService)
    {
        return messagingService.getLocalMessageQueueFactory();
    }

    @Bean(name = {"remoteMessageQueueFactory"})
    public MessageQueueFactory getRemoteMessageQueueFactory(
        RabbitMQMessagingService messagingService)
    {
        return messagingService.getRemoteMessageQueueFactory();
    }

    @Bean(name = "remoteActorSystemMessageQueueFactoryFactory")
    public MessageQueueFactoryFactory getRemoteActorSystemMessageQueueFactoryFactory(
        RabbitMQMessagingService messagingService)
    {
        return messagingService.getRemoteActorSystemMessageQueueFactoryFactory();
    }

    @Bean(name = "rabbitMQHealthCheck")
    public RabbitMQHealthCheck getHealthCheck(
        RabbitMQMessagingService messagingService)
    {
        return new RabbitMQHealthCheck(messagingService);
    }
}
