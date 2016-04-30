/*
 * Copyright 2013 - 2016 The Original Authors
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

package org.elasticsoftware.elasticactors.activemq;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.client.*;
import org.apache.activemq.artemis.core.remoting.impl.netty.NettyConnectorFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsoftware.elasticactors.PhysicalNode;
import org.elasticsoftware.elasticactors.messaging.*;
import org.elasticsoftware.elasticactors.messaging.MessageHandler;
import org.elasticsoftware.elasticactors.serialization.internal.InternalMessageDeserializer;
import org.elasticsoftware.elasticactors.util.concurrent.ThreadBoundExecutor;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.Maps.newHashMap;
import static java.lang.Integer.parseInt;
import static java.lang.String.format;
import static org.apache.activemq.artemis.api.core.SimpleString.toSimpleString;
import static org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants.*;
import static org.springframework.util.StringUtils.commaDelimitedListToSet;

/**
 * @author Joost van de Wijgerd
 */
public final class ActiveMQArtemisMessagingService implements MessagingService {
    private static final Logger logger = LogManager.getLogger(ActiveMQArtemisMessagingService.class);
    private static final String QUEUE_NAME_FORMAT = "%s/%s";
    private static final String EA_ADDRESS_FORMAT = "ea.%s";
    private static final int SERVER_DEFAULT_PORT = 61617;
    private final String activeMQHosts;
    private final String activeMQUsername;
    private final String activeMQPassword;
    private final String elasticActorsCluster;
    private final LocalMessageQueueFactory localMessageQueueFactory;
    private final RemoteMessageQueueFactory remoteMessageQueueFactory;
    private final RemoteActorSystemMessageQueueFactoryFactory remoteActorSystemMessageQueueFactoryFactory;
    private final ThreadBoundExecutor queueExecutor;
    private final InternalMessageDeserializer internalMessageDeserializer;
    private ClientSessionFactory clientSessionFactory;
    private ClientSession clientSession;
    private ClientProducer localClusterClientProducer;
    private final Map<String, ClientProducer> remoteClusterClientProducers = newHashMap();

    public ActiveMQArtemisMessagingService(String activeMQHosts, String activeMQUsername, String activeMQPassword,
                                           String elasticActorsCluster, ThreadBoundExecutor queueExecutor,
                                           InternalMessageDeserializer internalMessageDeserializer) {
        this.activeMQHosts = activeMQHosts;
        this.activeMQUsername = activeMQUsername;
        this.activeMQPassword = activeMQPassword;
        this.elasticActorsCluster = elasticActorsCluster;
        this.queueExecutor = queueExecutor;
        this.internalMessageDeserializer = internalMessageDeserializer;
        this.localMessageQueueFactory = new LocalMessageQueueFactory();
        this.remoteMessageQueueFactory = new RemoteMessageQueueFactory();
        this.remoteActorSystemMessageQueueFactoryFactory = new RemoteActorSystemMessageQueueFactoryFactory();
    }

    @PostConstruct
    public void start() throws Exception {
        Set<String> hosts = commaDelimitedListToSet(activeMQHosts);
        TransportConfiguration[] transportConfigurations = new TransportConfiguration[hosts.size()];
        int i = 0;
        // create the connections
        for (String hostAndPort : hosts) {
            int port = SERVER_DEFAULT_PORT;
            String host = null;
            // see if the port is specified
            int idx = hostAndPort.lastIndexOf(":");
            if(idx > -1) {
                port = parseInt(hostAndPort.substring(idx + 1));
                host = hostAndPort.substring(0, idx);
            } else {
                host = hostAndPort;
            }
            transportConfigurations[i++] = createConnector(host, port);
        }

        ServerLocator serverLocator = ActiveMQClient.createServerLocatorWithHA(transportConfigurations);
        // @todo: make this configurable to increase performance?
        serverLocator.setBlockOnDurableSend(true);
        serverLocator.setBlockOnNonDurableSend(false);
        serverLocator.setUseGlobalPools(true);
        //serverLocator.setAckBatchSize(1);
        serverLocator.setClientFailureCheckPeriod(4000L);
        //serverLocator.setConnectionTTL()
        serverLocator.setFailoverOnInitialConnection(true);
        serverLocator.setScheduledThreadPoolMaxSize(1);
        serverLocator.setThreadPoolMaxSize(3);
        serverLocator.setInitialConnectAttempts(-1);
        serverLocator.setMaxRetryInterval(32000L);
        serverLocator.setRetryInterval(1000L);
        serverLocator.setRetryIntervalMultiplier(2);
        serverLocator.setReconnectAttempts(-1);
        serverLocator.setConnectionTTL(-1);
        this.clientSessionFactory = serverLocator.createSessionFactory();
        this.clientSession = clientSessionFactory.createSession(activeMQUsername, activeMQPassword, false, true, true, false, serverLocator.getAckBatchSize());
        // this.clientSession.addFailoverListener();
        this.localClusterClientProducer = clientSession.createProducer(format(EA_ADDRESS_FORMAT, elasticActorsCluster));
        // need to start the clientSession
        clientSession.start();
    }

    private TransportConfiguration createConnector(String host, int port) {
        Map<String, Object> connectionParams = new HashMap<>();

        connectionParams.put(HOST_PROP_NAME, host);
        connectionParams.put(PORT_PROP_NAME, port);
        connectionParams.put(TCP_NODELAY_PROPNAME, true);
        // tuned for Gigabit switched single datacenter connections
        connectionParams.put(TCP_SENDBUFFER_SIZE_PROPNAME, 40000);
        connectionParams.put(TCP_RECEIVEBUFFER_SIZE_PROPNAME, 40000);
        // don't use NIO on the client
        connectionParams.put(USE_NIO_PROP_NAME, false);


        return new TransportConfiguration(NettyConnectorFactory.class.getName(), connectionParams);
    }

    @PreDestroy
    public void stop() {
        try {
            // close the remote producers
            for (ClientProducer clientProducer : remoteClusterClientProducers.values()) {
                clientProducer.close();
            }
            // close the local producer
            localClusterClientProducer.close();
            // end the session
            clientSession.stop();
        } catch (ActiveMQException e) {
            logger.error("Exception while stopping ClientSession" , e);
        }
    }

    @Override
    public void sendWireMessage(String queueName, byte[] serializedMessage, PhysicalNode receiver) throws IOException {
        // do nothing
    }

    private void ensureQueueExists(final ClientSession clientSession,final String queueName, final String routingKey) throws ActiveMQException {
        // ensure we have the queue created on the broker
        ClientSession.QueueQuery queueQuery = clientSession.queueQuery(toSimpleString(queueName));
        if(!queueQuery.isExists()) {
            // need to create it
            clientSession.createQueue(format(EA_ADDRESS_FORMAT, elasticActorsCluster), queueName, format("routingKey=%s", routingKey ), true);
        }
    }

    public MessageQueueFactory getLocalMessageQueueFactory() {
        return localMessageQueueFactory;
    }

    public MessageQueueFactory getRemoteMessageQueueFactory() {
        return remoteMessageQueueFactory;
    }

    public MessageQueueFactoryFactory getRemoteActorSystemMessageQueueFactoryFactory() {
        return remoteActorSystemMessageQueueFactoryFactory;
    }

    private final class LocalMessageQueueFactory implements MessageQueueFactory {
        @Override
        public MessageQueue create(String name, MessageHandler messageHandler) throws Exception {
            final String queueName = format(QUEUE_NAME_FORMAT,elasticActorsCluster,name);
            ensureQueueExists(clientSession, queueName, name);
            LocalMessageQueue messageQueue = new LocalMessageQueue(queueExecutor, internalMessageDeserializer,
                                                                   queueName, name, clientSession, localClusterClientProducer,
                                                                   messageHandler);
            messageQueue.initialize();
            return messageQueue;
        }
    }

    private final class RemoteMessageQueueFactory implements MessageQueueFactory {
        @Override
        public MessageQueue create(String name, MessageHandler messageHandler) throws Exception {
            final String queueName = format(QUEUE_NAME_FORMAT,elasticActorsCluster,name);
            ensureQueueExists(clientSession, queueName, name);
            return new RemoteMessageQueue(queueName, name, clientSession, localClusterClientProducer);
        }
    }

    private final class RemoteActorSystemMessageQueueFactory implements MessageQueueFactory {
        private final String clusterName;
        private final ClientProducer remoteClusterClientProducer;

        private RemoteActorSystemMessageQueueFactory(String clusterName, ClientProducer remoteClusterClientProducer) {
            this.clusterName = clusterName;
            this.remoteClusterClientProducer = remoteClusterClientProducer;
        }

        @Override
        public MessageQueue create(String name, MessageHandler messageHandler) throws Exception {
            final String queueName = format(QUEUE_NAME_FORMAT,this.clusterName,name);
            ensureQueueExists(clientSession, queueName, name);
            return new RemoteMessageQueue(queueName, name, clientSession, remoteClusterClientProducer);
        }
    }

    private final class RemoteActorSystemMessageQueueFactoryFactory implements MessageQueueFactoryFactory {
        @Override
        public MessageQueueFactory create(String clusterName) {
            try {
                ClientProducer remoteClusterClientProducer = remoteClusterClientProducers.get(clusterName);
                if(remoteClusterClientProducer == null) {
                    remoteClusterClientProducer = clientSession.createProducer(format(EA_ADDRESS_FORMAT, clusterName));
                    remoteClusterClientProducers.put(clusterName, remoteClusterClientProducer);
                }
                return new RemoteActorSystemMessageQueueFactory(clusterName, remoteClusterClientProducer);
            } catch(ActiveMQException e) {
                throw new MessagingServiceInitializationException(format("Exception initializating ClientProducer on Remote ActorSystem %s", clusterName), e, false);
            }
        }
    }
}
