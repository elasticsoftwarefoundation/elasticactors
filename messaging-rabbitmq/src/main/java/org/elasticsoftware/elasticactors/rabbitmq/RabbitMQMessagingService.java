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

package org.elasticsoftware.elasticactors.rabbitmq;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import net.jodah.lyra.ConnectionOptions;
import net.jodah.lyra.Connections;
import net.jodah.lyra.config.Config;
import net.jodah.lyra.config.RecoveryPolicy;
import net.jodah.lyra.config.RetryPolicy;
import net.jodah.lyra.util.Duration;
import org.apache.log4j.Logger;
import org.elasticsoftware.elasticactors.PhysicalNode;
import org.elasticsoftware.elasticactors.messaging.MessageHandler;
import org.elasticsoftware.elasticactors.messaging.MessageQueue;
import org.elasticsoftware.elasticactors.messaging.MessageQueueFactory;
import org.elasticsoftware.elasticactors.messaging.MessagingService;
import org.elasticsoftware.elasticactors.util.concurrent.ThreadBoundExecutor;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.IOException;

import static java.lang.String.format;

/**
 * @author Joost van de Wijgerd
 */
public class RabbitMQMessagingService implements MessagingService {
    private static final Logger logger = Logger.getLogger(RabbitMQMessagingService.class);
    private final ConnectionFactory connectionFactory = new ConnectionFactory();
    private final String rabbitmqHosts;
    private static final String QUEUE_NAME_FORMAT = "%s/%s";
    private final String elasticActorsCluster;
    private static final String EA_EXCHANGE_FORMAT = "ea.%s";
    private final String exchangeName;
    private Connection clientConnection;
    private Channel consumerChannel;
    private Channel producerChannel;
    private final LocalMessageQueueFactory localMessageQueueFactory;
    private final RemoteMessageQueueFactory remoteMessageQueueFactory;
    private final ThreadBoundExecutor<String> queueExecutor;
    private final String username;
    private final String password;

    public RabbitMQMessagingService(String elasticActorsCluster, String rabbitmqHosts, String username, String password, ThreadBoundExecutor<String> queueExecutor) {
        this.rabbitmqHosts = rabbitmqHosts;
        this.elasticActorsCluster = elasticActorsCluster;
        this.queueExecutor = queueExecutor;
        this.username = username;
        this.password = password;
        this.exchangeName = format(EA_EXCHANGE_FORMAT, elasticActorsCluster);
        this.localMessageQueueFactory = new LocalMessageQueueFactory();
        this.remoteMessageQueueFactory = new RemoteMessageQueueFactory();
    }

    @PostConstruct
    public void start() throws IOException {
        // millis
        connectionFactory.setConnectionTimeout(1000);
        // seconds
        connectionFactory.setRequestedHeartbeat(4);
        // lyra reconnect logic
        Config config = new Config()
                .withRecoveryPolicy(new RecoveryPolicy()
                        .withMaxAttempts(-1)
                        .withBackoff(Duration.seconds(1),Duration.seconds(30))
                ).withRetryPolicy(new RetryPolicy()
                        .withMaxAttempts(-1)
                        .withBackoff(Duration.seconds(1),Duration.seconds(30)));

        ConnectionOptions connectionOptions = new ConnectionOptions(connectionFactory).withAddresses(rabbitmqHosts);
        connectionOptions.withUsername(username);
        connectionOptions.withPassword(password);
        // create single connection
        //clientConnection = connectionFactory.newConnection(Address.parseAddresses(rabbitmqHosts));
        clientConnection = Connections.create(connectionOptions,config);
        // create a seperate producer and a seperate consumer channel
        consumerChannel = clientConnection.createChannel();
        producerChannel = clientConnection.createChannel();
        // ensure the exchange is there
        consumerChannel.exchangeDeclare(exchangeName,"direct",true);
    }

    @PreDestroy
    public void stop() {
        try {
            producerChannel.close();
            consumerChannel.close();
            clientConnection.close();
        } catch (IOException e) {
            logger.error("Failed to close all RabbitMQ Client resources",e);
        }
    }



    @Override
    public void sendWireMessage(String queueName, byte[] serializedMessage, PhysicalNode receiver) throws IOException {
        producerChannel.basicPublish(exchangeName,queueName,true,false,null,serializedMessage);
    }

    public MessageQueueFactory getLocalMessageQueueFactory() {
        return localMessageQueueFactory;
    }

    public MessageQueueFactory getRemoteMessageQueueFactory() {
        return remoteMessageQueueFactory;
    }

    private void ensureQueueExists(final Channel channel,final String queueName) throws IOException {
        // ensure we have the queue created on the broker
        AMQP.Queue.DeclareOk result = channel.queueDeclare(queueName, true, false, false, null);
        // and bound to the exchange
        channel.queueBind(queueName,exchangeName,queueName);
    }

    private final class LocalMessageQueueFactory implements MessageQueueFactory {
        @Override
        public MessageQueue create(String name, MessageHandler messageHandler) throws Exception {
            final String queueName = format(QUEUE_NAME_FORMAT,elasticActorsCluster,name);
            ensureQueueExists(consumerChannel,queueName);
            LocalMessageQueue messageQueue = new LocalMessageQueue(queueExecutor, consumerChannel,producerChannel,exchangeName,queueName,messageHandler);
            messageQueue.initialize();
            return messageQueue;
        }
    }

    private final class RemoteMessageQueueFactory implements MessageQueueFactory {
        @Override
        public MessageQueue create(String name, MessageHandler messageHandler) throws Exception {
            final String queueName = format(QUEUE_NAME_FORMAT,elasticActorsCluster,name);
            ensureQueueExists(producerChannel,queueName);
            return new RemoteMessageQueue(producerChannel,exchangeName,queueName);
        }
    }

    private final class RemoteActorSystemMessageQueueFactory implements MessageQueueFactory {
        private final String clusterName;

        private RemoteActorSystemMessageQueueFactory(String clusterName) {
            this.clusterName = clusterName;
        }

        @Override
        public MessageQueue create(String name, MessageHandler messageHandler) throws Exception {
            final String queueName = format(QUEUE_NAME_FORMAT,this.clusterName,name);
            ensureQueueExists(producerChannel,queueName);
            return new RemoteMessageQueue(producerChannel,exchangeName,queueName);
        }
    }
}
