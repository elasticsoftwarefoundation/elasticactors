/*
 * Copyright 2013 - 2014 The Original Authors
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

import org.elasticsoftware.elasticactors.messaging.MessageQueueFactory;
import org.elasticsoftware.elasticactors.messaging.MessagingService;
import org.elasticsoftware.elasticactors.rabbitmq.RabbitMQMessagingService;
import org.elasticsoftware.elasticactors.util.concurrent.ThreadBoundExecutor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;

import javax.annotation.PostConstruct;

/**
 * @author Joost van de Wijgerd
 */
public class MessagingConfiguration {
    @Autowired
    private Environment env;
    @Autowired @Qualifier("queueExecutor")
    private ThreadBoundExecutor<String> queueExecutor;
    private RabbitMQMessagingService messagingService;

    @PostConstruct
    public void initialize() {
        String clusterName = env.getRequiredProperty("ea.cluster");
        String rabbitMQHosts = env.getRequiredProperty("ea.rabbitmq.hosts");
        String rabbitMQUsername= env.getProperty("ea.rabbitmq.username","guest");
        String rabbitMQPassword = env.getProperty("ea.rabbitmq.password","guest");
        messagingService = new RabbitMQMessagingService(clusterName,rabbitMQHosts, rabbitMQUsername, rabbitMQPassword,queueExecutor);
    }

    @Bean(name = {"messagingService"})
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
}
