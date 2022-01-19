/*
 *   Copyright 2013 - 2022 The Original Authors
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

import org.elasticsoftware.elasticactors.activemq.ActiveMQArtemisMessagingService;
import org.elasticsoftware.elasticactors.cluster.ActorRefFactory;
import org.elasticsoftware.elasticactors.cluster.InternalActorSystem;
import org.elasticsoftware.elasticactors.messaging.MessageQueueFactory;
import org.elasticsoftware.elasticactors.messaging.MessageQueueFactoryFactory;
import org.elasticsoftware.elasticactors.serialization.internal.ActorRefDeserializer;
import org.elasticsoftware.elasticactors.serialization.internal.InternalMessageDeserializer;
import org.elasticsoftware.elasticactors.util.concurrent.ThreadBoundExecutor;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;

/**
 * @author Joost van de Wijgerd
 */
public class MessagingConfiguration {

    @Bean(name = {"messagingService"})
    public ActiveMQArtemisMessagingService getMessagingService(
        @Qualifier("queueExecutor") ThreadBoundExecutor queueExecutor,
        InternalActorSystem internalActorSystem,
        ActorRefFactory actorRefFactory,
        Environment env)
    {
        String clusterName = env.getRequiredProperty("ea.cluster");
        String activeMQHosts = env.getRequiredProperty("ea.activemq.hosts");
        String activeMQUsername = env.getProperty("ea.activemq.username", "guest");
        String activeMQPassword = env.getProperty("ea.activemq.password", "guest");
        boolean useMessageHandler =
            env.getProperty("ea.activemq.useMessageHandler", Boolean.TYPE, true);
        boolean useReceiveImmediate =
            env.getProperty("ea.activemq.useReceiveImmediate", Boolean.TYPE, false);
        return new ActiveMQArtemisMessagingService(
            activeMQHosts,
            activeMQUsername,
            activeMQPassword,
            clusterName,
            queueExecutor,
            new InternalMessageDeserializer(
                new ActorRefDeserializer(actorRefFactory),
                internalActorSystem
            ),
            useMessageHandler,
            useReceiveImmediate
        );
    }

    @Bean(name = {"localMessageQueueFactory"})
    public MessageQueueFactory getLocalMessageQueueFactory(
        ActiveMQArtemisMessagingService messagingService)
    {
        return messagingService.getLocalMessageQueueFactory();
    }

    @Bean(name = {"remoteMessageQueueFactory"})
    public MessageQueueFactory getRemoteMessageQueueFactory(
        ActiveMQArtemisMessagingService messagingService)
    {
        return messagingService.getRemoteMessageQueueFactory();
    }

    @Bean(name = "remoteActorSystemMessageQueueFactoryFactory")
    public MessageQueueFactoryFactory getRemoteActorSystemMessageQueueFactoryFactory(
        ActiveMQArtemisMessagingService messagingService)
    {
        return messagingService.getRemoteActorSystemMessageQueueFactoryFactory();
    }
}
