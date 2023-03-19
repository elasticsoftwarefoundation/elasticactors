/*
 * Copyright 2013 - 2023 The Original Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */

package org.elasticsoftware.elasticactors.rabbitmq.sc;

import com.google.common.base.Throwables;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MetricsCollector;
import com.rabbitmq.client.impl.MicrometerMetricsCollector;
import net.jodah.lyra.ConnectionOptions;
import net.jodah.lyra.Connections;
import net.jodah.lyra.config.Config;
import net.jodah.lyra.config.RecoveryPolicy;
import net.jodah.lyra.event.ChannelListener;
import net.jodah.lyra.util.Duration;
import org.elasticsoftware.elasticactors.PhysicalNode;
import org.elasticsoftware.elasticactors.cluster.metrics.MicrometerConfiguration;
import org.elasticsoftware.elasticactors.messaging.MessageHandler;
import org.elasticsoftware.elasticactors.messaging.MessageQueue;
import org.elasticsoftware.elasticactors.messaging.MessageQueueFactory;
import org.elasticsoftware.elasticactors.messaging.MessageQueueFactoryFactory;
import org.elasticsoftware.elasticactors.rabbitmq.ChannelListenerRegistry;
import org.elasticsoftware.elasticactors.rabbitmq.LoggingShutdownListener;
import org.elasticsoftware.elasticactors.rabbitmq.MessageAcker;
import org.elasticsoftware.elasticactors.rabbitmq.RabbitMQMessagingService;
import org.elasticsoftware.elasticactors.rabbitmq.ack.AsyncMessageAcker;
import org.elasticsoftware.elasticactors.rabbitmq.ack.BufferingMessageAcker;
import org.elasticsoftware.elasticactors.rabbitmq.ack.DirectMessageAcker;
import org.elasticsoftware.elasticactors.rabbitmq.ack.WriteBehindMessageAcker;
import org.elasticsoftware.elasticactors.serialization.internal.InternalMessageDeserializer;
import org.elasticsoftware.elasticactors.util.concurrent.ThreadBoundExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

import jakarta.annotation.Nullable;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

import static org.elasticsoftware.elasticactors.rabbitmq.MessageAcker.Type.ASYNC;
import static org.elasticsoftware.elasticactors.rabbitmq.MessageAcker.Type.BUFFERED;
import static org.elasticsoftware.elasticactors.rabbitmq.MessageAcker.Type.WRITE_BEHIND;

import static java.lang.String.format;

/**
 * @author Joost van de Wijgerd
 */
public final class SingleProducerRabbitMQMessagingService
    implements RabbitMQMessagingService, ChannelListenerRegistry, ChannelListener {

    private static final Logger logger = LoggerFactory.getLogger(
        SingleProducerRabbitMQMessagingService.class);

    private final ConnectionFactory connectionFactory = new ConnectionFactory();
    private final String rabbitmqHosts;
    private final Integer rabbitmqPort;
    private static final String QUEUE_NAME_FORMAT = "%s/%s";
    private final String elasticActorsCluster;
    private static final String EA_EXCHANGE_FORMAT = "ea.%s";
    private final String exchangeName;
    private Connection clientConnection;
    private Channel consumerChannel;
    private Channel producerChannel;
    private final LocalMessageQueueFactory localMessageQueueFactory;
    private final RemoteMessageQueueFactory remoteMessageQueueFactory;
    private final RemoteActorSystemMessageQueueFactoryFactory remoteActorSystemMessageQueueFactoryFactory;
    private final ThreadBoundExecutor queueExecutor;
    private final String username;
    private final String password;
    private final InternalMessageDeserializer internalMessageDeserializer;
    private final ConcurrentMap<Channel,Set<ChannelListener>> channelListenerRegistry = new ConcurrentHashMap<>();
    private final MessageAcker.Type ackType;
    private MessageAcker messageAcker;
    private final Integer prefetchCount;
    private final MicrometerConfiguration micrometerConfiguration;
    private final MicrometerConfiguration ackerMicrometerConfiguration;

    public SingleProducerRabbitMQMessagingService(
        String elasticActorsCluster,
        String rabbitmqHosts,
        Integer rabbitmqPort, 
        String username,
        String password,
        MessageAcker.Type ackType,
        ThreadBoundExecutor queueExecutor,
        InternalMessageDeserializer internalMessageDeserializer,
        Integer prefetchCount,
        @Nullable MicrometerConfiguration micrometerConfiguration,
        @Nullable MicrometerConfiguration ackerMicrometerConfiguration)
    {
        this.rabbitmqHosts = rabbitmqHosts;
        this.elasticActorsCluster = elasticActorsCluster;
        this.rabbitmqPort = rabbitmqPort;
        this.queueExecutor = queueExecutor;
        this.username = username;
        this.password = password;
        this.ackType = ackType;
        this.internalMessageDeserializer = internalMessageDeserializer;
        this.exchangeName = format(EA_EXCHANGE_FORMAT, elasticActorsCluster);
        this.prefetchCount = prefetchCount;
        this.localMessageQueueFactory = new LocalMessageQueueFactory();
        this.remoteMessageQueueFactory = new RemoteMessageQueueFactory();
        this.remoteActorSystemMessageQueueFactoryFactory = new RemoteActorSystemMessageQueueFactoryFactory();
        this.micrometerConfiguration = micrometerConfiguration;
        this.ackerMicrometerConfiguration = ackerMicrometerConfiguration;
    }

    @PostConstruct
    public void start() throws IOException, TimeoutException {
        logger.info("Starting messaging service [{}]", getClass().getSimpleName());
        if (micrometerConfiguration != null) {
            MetricsCollector metricsCollector = new MicrometerMetricsCollector(
                micrometerConfiguration.getRegistry(),
                micrometerConfiguration.getMetricPrefix() + "rabbitmq",
                micrometerConfiguration.getTags()
            );
            connectionFactory.setMetricsCollector(metricsCollector);
        }
        // millis
        connectionFactory.setConnectionTimeout(1000);
        // seconds
        connectionFactory.setRequestedHeartbeat(4);
        // turn off recovery as we are using Lyra
        connectionFactory.setAutomaticRecoveryEnabled(false);
        connectionFactory.setTopologyRecoveryEnabled(false);
        // lyra reconnect logic
        Config config = new Config()
                .withRecoveryPolicy(new RecoveryPolicy()
                        .withMaxAttempts(-1)
                        .withInterval(Duration.seconds(1)))
                .withChannelListeners(this);

        ConnectionOptions connectionOptions = new ConnectionOptions(connectionFactory)
                .withHosts(StringUtils.commaDelimitedListToStringArray(rabbitmqHosts))
                .withPort(rabbitmqPort)
                .withUsername(username)
                .withPassword(password);
        // create single connection
        //clientConnection = connectionFactory.newConnection(Address.parseAddresses(rabbitmqHosts));
        clientConnection = Connections.create(connectionOptions,config);
        // create a seperate producer and a seperate consumer channel
        consumerChannel = clientConnection.createChannel();
        consumerChannel.basicQos(prefetchCount);
        producerChannel = clientConnection.createChannel();
        // add logging shutdown listener
        consumerChannel.addShutdownListener(LoggingShutdownListener.INSTANCE);
        producerChannel.addShutdownListener(LoggingShutdownListener.INSTANCE);
        // ensure the exchange is there
        consumerChannel.exchangeDeclare(exchangeName,"direct",true);
        if(ackType == BUFFERED) {
            messageAcker = new BufferingMessageAcker(consumerChannel);
        } else if(ackType == WRITE_BEHIND) {
            messageAcker = new WriteBehindMessageAcker(consumerChannel);
        } else if(ackType == ASYNC) {
            messageAcker = new AsyncMessageAcker(consumerChannel, ackerMicrometerConfiguration);
        } else {
            messageAcker = new DirectMessageAcker(consumerChannel);
        }
        messageAcker.start();
    }

    @PreDestroy
    public void stop() {
        logger.info("Stopping messaging service");
        try {
            messageAcker.stop();
            producerChannel.close();
            consumerChannel.close();
            clientConnection.close();
        } catch (IOException|TimeoutException e) {
            logger.error("Failed to close all RabbitMQ Client resources",e);
        }
    }

    @Override
    public void sendWireMessage(String queueName, byte[] serializedMessage, PhysicalNode receiver) throws IOException {
        producerChannel.basicPublish(exchangeName,queueName,true,false,null,serializedMessage);
    }

    @Override
    public MessageQueueFactory getLocalMessageQueueFactory() {
        return localMessageQueueFactory;
    }

    @Override
    public MessageQueueFactory getRemoteMessageQueueFactory() {
        return remoteMessageQueueFactory;
    }

    @Override
    public MessageQueueFactoryFactory getRemoteActorSystemMessageQueueFactoryFactory() {
        return remoteActorSystemMessageQueueFactoryFactory;
    }

    @Override
    public void addChannelListener(final Channel channel,final ChannelListener channelListener) {
        Set<ChannelListener> listeners = this.channelListenerRegistry.get(channel);
        if(listeners == null) {
            listeners = Collections.newSetFromMap(new ConcurrentHashMap<>());
            if(this.channelListenerRegistry.putIfAbsent(channel,listeners) != null) {
                // was already created
                listeners = this.channelListenerRegistry.get(channel);
            }
        }
        listeners.add(channelListener);
    }

    @Override
    public void removeChannelListener(final Channel channel,final ChannelListener channelListener) {
        final Set<ChannelListener> listeners = this.channelListenerRegistry.get(channel);
        if(listeners != null) {
            listeners.remove(channelListener);
        }
    }

    @Override
    public void onCreate(final Channel channel) {
        propagateChannelEvent(channel, c -> c.onCreate(channel), "onCreate");
    }

    @Override
    public void onCreateFailure(final Throwable failure) {
        logger.error("Channel creation failed, reason: " + System.lineSeparator() + Throwables.getStackTraceAsString(failure));
    }

    @Override
    public void onRecoveryStarted(final Channel channel) {
        propagateChannelEvent(channel, c -> c.onRecoveryStarted(channel), "onRecoveryStarted");
    }

    @Override
    public void onRecovery(final Channel channel) {
        propagateChannelEvent(channel, c -> c.onRecovery(channel), "onRecovery");
    }

    @Override
    public void onRecoveryCompleted(final Channel channel) {
        propagateChannelEvent(channel, c -> c.onRecoveryCompleted(channel), "onRecoveryCompleted");
    }

    @Override
    public void onRecoveryFailure(final Channel channel, final Throwable failure) {
        propagateChannelEvent(channel, c -> c.onRecoveryFailure(channel, failure), "onRecoveryFailure");
    }

    @Override
    public boolean isClientConnectionOpen() {
        return clientConnection != null && clientConnection.isOpen();
    }

    @Override
    public boolean areConsumerChannelsOpen() {
        return consumerChannel != null && consumerChannel.isOpen();
    }

    @Override
    public boolean areProducerChannelsOpen() {
        return producerChannel != null && producerChannel.isOpen();
    }

    private void propagateChannelEvent(final Channel channel, final Consumer<ChannelListener> channelEvent, final String channelEventName) {
        final Set<ChannelListener> listeners = this.channelListenerRegistry.get(channel);
        if(listeners != null) {
            for (ChannelListener listener : listeners) {
                try {
                    channelEvent.accept(listener);
                } catch(Exception e) {
                    logger.error("Exception while calling [{}] on ChannelListener [{}]", channelEventName, listener,e);
                }
            }
        }
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
            LocalMessageQueue messageQueue = new LocalMessageQueue(queueExecutor,
                    SingleProducerRabbitMQMessagingService.this,
                    consumerChannel,
                    producerChannel,
                    exchangeName,queueName,messageHandler,
                    internalMessageDeserializer, messageAcker);
            messageQueue.initialize();
            return messageQueue;
        }
    }

    private final class RemoteMessageQueueFactory implements MessageQueueFactory {
        @Override
        public MessageQueue create(String name, MessageHandler messageHandler) throws Exception {
            final String queueName = format(QUEUE_NAME_FORMAT,elasticActorsCluster,name);
            ensureQueueExists(producerChannel,queueName);
            return new RemoteMessageQueue(SingleProducerRabbitMQMessagingService.this,producerChannel,exchangeName,queueName);
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
            RemoteMessageQueue messageQueue =
                new RemoteMessageQueue(
                    SingleProducerRabbitMQMessagingService.this,
                    producerChannel,
                    exchangeName,
                    queueName);
            messageQueue.initialize();
            return messageQueue;
        }
    }

    private final class RemoteActorSystemMessageQueueFactoryFactory implements MessageQueueFactoryFactory {
        @Override
        public MessageQueueFactory create(String clusterName) {
            return new RemoteActorSystemMessageQueueFactory(clusterName);
        }
    }
}
