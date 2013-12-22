package org.elasticsoftware.elasticactors.rabbitmq;

import com.rabbitmq.client.*;
import org.apache.log4j.Logger;
import org.elasticsoftware.elasticactors.PhysicalNode;
import org.elasticsoftware.elasticactors.messaging.MessageHandler;
import org.elasticsoftware.elasticactors.messaging.MessageQueue;
import org.elasticsoftware.elasticactors.messaging.MessageQueueFactory;
import org.elasticsoftware.elasticactors.messaging.MessagingService;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.IOException;

/**
 * @author Joost van de Wijgerd
 */
public class RabbitMQMessagingService implements MessagingService {
    private static final Logger logger = Logger.getLogger(RabbitMQMessagingService.class);
    private final ConnectionFactory connectionFactory = new ConnectionFactory();
    private final String rabbitmqHosts;
    private final String elasticActorsCluster;
    private static final String EA_EXCHANGE_FORMAT = "ea.%s";
    private final String exchangeName;
    private Connection clientConnection;
    private Channel consumerChannel;
    private Channel producerChannel;
    private final LocalMessageQueueFactory localMessageQueueFactory;
    private final RemoteMessageQueueFactory remoteMessageQueueFactory;

    public RabbitMQMessagingService(String elasticActorsCluster,String rabbitmqHosts) {
        this.rabbitmqHosts = rabbitmqHosts;
        this.elasticActorsCluster = elasticActorsCluster;
        this.exchangeName = String.format(EA_EXCHANGE_FORMAT,elasticActorsCluster);
        this.localMessageQueueFactory = new LocalMessageQueueFactory();
        this.remoteMessageQueueFactory = new RemoteMessageQueueFactory();
    }

    @PostConstruct
    public void start() throws IOException {
        // millis
        connectionFactory.setConnectionTimeout(1000);
        // seconds
        connectionFactory.setRequestedHeartbeat(4);
        // create single connection
        clientConnection = connectionFactory.newConnection(Address.parseAddresses(rabbitmqHosts));
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
            ensureQueueExists(consumerChannel,name);
            LocalMessageQueue messageQueue = new LocalMessageQueue(consumerChannel,producerChannel,exchangeName,name,messageHandler);
            messageQueue.initialize();
            return messageQueue;
        }
    }

    private final class RemoteMessageQueueFactory implements MessageQueueFactory {
        @Override
        public MessageQueue create(String name, MessageHandler messageHandler) throws Exception {
            ensureQueueExists(producerChannel,name);
            return new RemoteMessageQueue(producerChannel,exchangeName,name);
        }
    }
}
