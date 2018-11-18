package org.elasticsoftware.elasticactors.client.rabbitmq;

import com.google.common.base.Charsets;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.rabbitmq.client.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsoftware.elasticactors.*;
import org.elasticsoftware.elasticactors.client.common.ActorSystemWrapper;
import org.elasticsoftware.elasticactors.client.common.ThreadBoundRunnableWrapper;
import org.elasticsoftware.elasticactors.cluster.ActorRefFactory;
import org.elasticsoftware.elasticactors.cluster.MessageSerializationRegistry;
import org.elasticsoftware.elasticactors.cluster.SerializationFrameworkRegistry;
import org.elasticsoftware.elasticactors.core.actors.CompletableFutureDelegate;
import org.elasticsoftware.elasticactors.core.actors.ReplyActor;
import org.elasticsoftware.elasticactors.messaging.InternalMessage;
import org.elasticsoftware.elasticactors.messaging.InternalMessageImpl;
import org.elasticsoftware.elasticactors.serialization.*;
import org.elasticsoftware.elasticactors.serialization.internal.ActorRefDeserializer;
import org.elasticsoftware.elasticactors.serialization.internal.InternalMessageDeserializer;
import org.elasticsoftware.elasticactors.util.concurrent.DaemonThreadFactory;
import org.elasticsoftware.elasticactors.util.concurrent.ThreadBoundExecutor;
import org.elasticsoftware.elasticactors.util.concurrent.ThreadBoundExecutorImpl;
import org.elasticsoftware.elasticactors.util.concurrent.ThreadBoundRunnable;
import org.reactivestreams.Publisher;
import org.springframework.beans.factory.ListableBeanFactory;

import javax.annotation.Nullable;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import static java.lang.String.format;

public final class RabbitMQActorSystemClient implements ActorSystemClient, ActorRefFactory, SerializationFrameworkRegistry, MessageSerializationRegistry {
    private static final Logger logger = LogManager.getLogger(RabbitMQActorSystemClient.class);
    private static final String EXCEPTION_FORMAT = "Invalid ActorRef, required spec: [actor://<cluster>/<actorSystem>/[shards|nodes|services|clients]/<shardId>/<actorId (optional)>, actual spec: [%s]";
    private static final String QUEUE_NAME_FORMAT = "%s/%s";
    private static final String EA_EXCHANGE_FORMAT = "ea.%s";
    private static final String CLIENT_QUEUE_FORMAT = "%s/clients/%s";
    private static final String SHARD_QUEUE_FORMAT = "%s/shards/%s";
    private final HashFunction hashFunction = Hashing.murmur3_32();
    private final String clusterName;
    private final String actorSystemName;
    private final Integer numberOfShards;
    private final String nodeId;
    private final String exchangeName;
    private final String clientQueue;
    private final InternalMessageDeserializer internalMessageDeserializer;
    private final ThreadBoundExecutor producerExecutor = new ThreadBoundExecutorImpl(new DaemonThreadFactory("RabbitMQActorSystemClient-Producer"), 1);
    private final ThreadBoundExecutor consumerExecutor = new ThreadBoundExecutorImpl(new DaemonThreadFactory("RabbitMQActorSystemClient-Producer"), 1);
    private final Map<String, ClientActor> localActors = new HashMap<>();
    private final ActorSystem actorSystem = new ActorSystemWrapper(this);
    private final SystemSerializers systemSerializers;
    private final SystemDeserializers systemDeserializers;
    private final ListableBeanFactory beanFactory;
    private final Cache<String,ActorRef> actorRefCache;
    private final String rabbitmqHosts;
    private final String username;
    private final String password;
    private Collection<SerializationFramework> serializationFrameworks;
    private Connection clientConnection;
    private Channel consumerChannel;
    private Channel producerChannel;

    public RabbitMQActorSystemClient(String clusterName,
                                     String actorSystemName,
                                     Integer numberOfShards,
                                     String nodeId,
                                     String rabbitmqHosts,
                                     String username,
                                     String password,
                                     ListableBeanFactory beanFactory,
                                     long actorRefCacheMaxSize) {
        this.clusterName = clusterName;
        this.actorSystemName = actorSystemName;
        this.numberOfShards = numberOfShards;
        this.nodeId = nodeId;
        this.exchangeName = format(EA_EXCHANGE_FORMAT, clusterName);
        this.clientQueue = format(CLIENT_QUEUE_FORMAT, actorSystemName, nodeId);
        this.internalMessageDeserializer = new InternalMessageDeserializer(new ActorRefDeserializer(this), this);
        this.systemDeserializers = new SystemDeserializers(this, this,this);
        this.systemSerializers = new SystemSerializers(this, this);
        this.beanFactory = beanFactory;
        this.actorRefCache = CacheBuilder.newBuilder().maximumSize(actorRefCacheMaxSize).build();
        this.rabbitmqHosts = rabbitmqHosts;
        this.username = username;
        this.password = password;
    }

    @Override
    public ActorRef create(String refSpec) {
        ActorRef cachedRef = actorRefCache.getIfPresent(refSpec);
        if(cachedRef != null) {
            return cachedRef;
        } else if (refSpec.startsWith("actor://")) {
            Scanner scanner = new Scanner(refSpec.substring(8));
            scanner.useDelimiter("/");
            String clusterName = scanner.next();
            String actorSystemName = scanner.next();
            String type = scanner.next();
            String shardOrNode = scanner.next();
            String actorId = scanner.next();
            if("clients".equals(type)) {
                return new LocalActorRef(actorId);
            } else if("shards".equals(type)) {
                return new RemoteActorRef(actorId);
            } else {
                // other types of actors are currently not supported
                throw new IllegalArgumentException("");
            }
        } else {
            throw new IllegalArgumentException(format(EXCEPTION_FORMAT, refSpec));
        }
    }

    @Override
    public SerializationFramework getSerializationFramework(Class<? extends SerializationFramework> frameworkClass) {
        return serializationFrameworks.stream().filter(framework -> frameworkClass.equals(framework.getClass())).findFirst().orElse(null);
    }

    @Override
    public <T> MessageSerializer<T> getSerializer(Class<T> messageClass) {
        MessageSerializer<T> messageSerializer = systemSerializers.get(messageClass);
        if(messageSerializer == null) {
            Message messageAnnotation = messageClass.getAnnotation(Message.class);
            if(messageAnnotation != null) {
                SerializationFramework framework = getSerializationFramework(messageAnnotation.serializationFramework());
                messageSerializer = framework.getSerializer(messageClass);
            }
        }
        return messageSerializer;
    }

    @Override
    public <T> MessageDeserializer<T> getDeserializer(Class<T> messageClass) {
        MessageDeserializer<T> messageDeserializer = systemDeserializers.get(messageClass);
        if(messageDeserializer == null) {
            Message messageAnnotation = messageClass.getAnnotation(Message.class);
            if(messageAnnotation != null) {
                SerializationFramework framework = getSerializationFramework(messageAnnotation.serializationFramework());
                messageDeserializer = framework.getDeserializer(messageClass);
            }
        }
        return messageDeserializer;
    }

    @PostConstruct
    public void start() throws Exception {
        this.serializationFrameworks = beanFactory.getBeansOfType(SerializationFramework.class).values();

        ConnectionFactory connectionFactory = new ConnectionFactory();
        // millis
        connectionFactory.setConnectionTimeout(1000);
        // seconds
        connectionFactory.setRequestedHeartbeat(4);
        connectionFactory.setUsername(username);
        connectionFactory.setPassword(password);

        clientConnection = connectionFactory.newConnection(Address.parseAddresses(rabbitmqHosts));
        // create a separate producer channel
        producerChannel = clientConnection.createChannel();

        consumerChannel = clientConnection.createChannel();
        consumerChannel.basicQos(20);

        // start consuming on my node queue
        // ensure the exchange is there
        consumerChannel.exchangeDeclare(exchangeName, "direct", true);
        String queueName = format(clientQueue, actorSystemName, nodeId);
        // ensure we have the queue created on the broker
        AMQP.Queue.DeclareOk result = consumerChannel.queueDeclare(queueName, true, false, false, null);
        // and bound to the exchange
        consumerChannel.queueBind(queueName, exchangeName, queueName);
        // now we can start consuming
        consumerChannel.basicConsume(queueName, false, this::handleDelivery, this::handleCancel);
    }

    @PreDestroy
    public void stop() {
        try {
            consumerChannel.close();
            producerChannel.close();
            clientConnection.close();
        } catch (Exception e) {
            logger.error("Unexpected Exception while closing RabbitMQ Client resources", e);
        }
    }

    private void handleDelivery(String consumerTag, Delivery delivery) {
        // delivery contains the payload
        consumerExecutor.execute(run(clientQueue, () -> handle(delivery)));
    }

    private void handle(Delivery delivery) {
        try {
            InternalMessage internalMessage = internalMessageDeserializer.deserialize(delivery.getBody());
            Class<?> messageClass = Class.forName(internalMessage.getPayloadClass());
            MessageDeserializer<?> deserializer = getDeserializer(messageClass);
            Object message = internalMessage.getPayload(deserializer);
            internalMessage.getReceivers().forEach(receiver -> {
                if(internalMessage.isUndeliverable()) {
                    handleMessageUndeliverable(receiver, internalMessage.getSender(), message);
                } else {
                    handleMessageReceived(receiver, internalMessage.getSender(), message);
                }
            });
        } catch(IOException e) {

        } catch(ClassNotFoundException e) {

        } finally {
            try {
                consumerChannel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
            } catch (IOException e) {
                logger.error("Unexpected Exception while acking delivery", e);
            }
        }
    }

    private void handleMessageReceived(ActorRef receiver, ActorRef sender, Object message) {
        // get the ClientActor instance
        ClientActor clientActor = localActors.get(receiver.getActorId());
        InternalActorContext.setContext(clientActor);
        try {
            clientActor.getElasticActor().onReceive(sender, message);
        } catch(Exception e) {
            logger.info("Unexpected Exception in onReceive", e);
        } finally {
            InternalActorContext.getAndClearContext();
        }
    }

    private void handleMessageUndeliverable(ActorRef receiver, ActorRef sender, Object originalMessage) {
        // get the ClientActor instance
        ClientActor clientActor = localActors.get(receiver.getActorId());
        InternalActorContext.setContext(clientActor);
        try {
            clientActor.getElasticActor().onUndeliverable(sender, originalMessage);
        } catch(Exception e) {
            logger.info("Unexpected Exception in onReceive", e);
        } finally {
            InternalActorContext.getAndClearContext();
        }
    }

    private void handleCancel(String consumerTag) {

    }

    private ThreadBoundRunnable<String> run(String key, Runnable r) {
        return new ThreadBoundRunnableWrapper(key, r);
    }

    @Override
    public String getClusterName() {
        return clusterName;
    }

    @Override
    public String getName() {
        return actorSystemName;
    }

    @Override
    public <T extends ElasticActor> ActorRef tempActorOf(Class<T> actorClass, @Nullable ActorState initialState) throws Exception {
        if(actorClass.getAnnotation(TempActor.class) == null) {
            throw new IllegalArgumentException("actorClass has to be annotated with @TempActor");
        }
        // if we have state we need to wrap it
        String actorId = UUID.randomUUID().toString();
        ActorRef ref = new LocalActorRef(actorId);

        ClientActor clientActor = new ClientActor(ref, actorClass.newInstance(), initialState, actorSystem);
        // cache the reference
        actorRefCache.put(ref.toString(), ref);
        // store it on the consumer executor to avoid having to use a concurrentmap
        consumerExecutor.execute(run(clientQueue, () -> localActors.put(actorId, clientActor)));

        return ref;
    }

    @Override
    public ActorRef actorFor(String actorId) {
        return new RemoteActorRef(actorId);
    }

    @Override
    public void stop(ActorRef actorRef) {
        if(actorRef instanceof LocalActorRef) {
            // remove from local actors and cache
            consumerExecutor.execute(run(clientQueue, () -> localActors.remove(actorRef.getActorId())));
            actorRefCache.invalidate(actorRef.toString());
        } else {
            throw new IllegalArgumentException("Cannot stop an Actor in the cluster");
        }
    }

    private Integer shardFor(String actorId) {
        return Math.abs(hashFunction.hashString(actorId, Charsets.UTF_8).asInt()) % numberOfShards;
    }

    private AMQP.BasicProperties createProps(InternalMessage message) {
        if (message.getTimeout() < 0) {
            return message.isDurable() ? MessageProperties.PERSISTENT_BASIC : MessageProperties.BASIC;
        } else {
            if (message.isDurable()) {
                return new AMQP.BasicProperties.Builder().contentType("application/octet-stream").deliveryMode(2)
                        .priority(0).expiration(String.valueOf(message.getTimeout())).build();
            } else {
                return new AMQP.BasicProperties.Builder().contentType("application/octet-stream").deliveryMode(1)
                        .priority(0).expiration(String.valueOf(message.getTimeout())).build();
            }
        }
    }

    private final class RemoteActorRef implements ActorRef {
        private static final String REFSPEC_FORMAT = "actor://%s/%s/%s";
        private final String actorId;
        private final String actorPath;
        private final String refSpec;

        private RemoteActorRef(String actorId) {
            this.actorId = actorId;
            this.actorPath = format(SHARD_QUEUE_FORMAT, actorSystemName, shardFor(actorId));
            this.refSpec = format(REFSPEC_FORMAT, clusterName, actorPath, actorId);
        }

        @Override
        public String getActorCluster() {
            return clusterName;
        }

        @Override
        public String getActorPath() {
            return actorPath;
        }

        @Override
        public String getActorId() {
            return actorId;
        }

        @Override
        public void tell(Object message, ActorRef sender) throws MessageDeliveryException {
            MessageSerializer messageSerializer = getSerializer(message.getClass());
            // get the durable flag
            Message messageAnnotation = message.getClass().getAnnotation(Message.class);
            final boolean durable = (messageAnnotation == null) || messageAnnotation.durable();
            final int timeout = (messageAnnotation != null) ? messageAnnotation.timeout() : Message.NO_TIMEOUT;
            // create the internal message
            try {
                InternalMessage internalMessage = new InternalMessageImpl(sender, ImmutableList.of(this),
                        SerializationContext.serialize(messageSerializer, message),
                        message.getClass().getName(), durable, timeout);

                final AMQP.BasicProperties props = createProps(internalMessage);
                // always publish on the same thread
                producerExecutor.execute(run(actorPath, () -> {
                    try {
                        producerChannel.basicPublish(exchangeName, actorPath, false, false, props, internalMessage.toByteArray());
                    } catch(IOException e) {
                        logger.error("Unexpected Exception while publishing message");
                    }
                }));
            } catch (IOException e) {
                throw new MessageDeliveryException("IOException while publishing message", e, false);
            }

        }

        @Override
        public void tell(Object message) throws IllegalStateException, MessageDeliveryException {
            final ActorRef self = ActorContextHolder.getSelf();
            if (self != null) {
                tell(message, self);
            } else {
                throw new IllegalStateException("Cannot determine ActorRef(self) Only use this method while inside an ElasticActor Lifecycle or on(Message) method!");
            }
        }

        @Override
        public <T> CompletionStage<T> ask(Object message, Class<T> responseType) {
            return ask(message, responseType, Boolean.FALSE);
        }

        @Override
        public <T> CompletionStage<T> ask(Object message, Class<T> responseType, Boolean persistOnResponse) {
            final CompletableFuture<T> future = new CompletableFuture<>();
            try {
                ActorRef replyRef = tempActorOf(ReplyActor.class,
                        new CompletableFutureDelegate<>(future, responseType, null));
                this.tell(message, replyRef);
            } catch (Exception e) {
                future.completeExceptionally(e);
            }
            return future;
        }

        @Override
        public boolean isLocal() {
            return false;  // by definition this is not a local Actor
        }

        @Override
        public <T> Publisher<T> publisherOf(Class<T> messageClass) {
            throw new UnsupportedOperationException("Not Supported in ActorSystemClient implementation");
        }

        @Override
        public final boolean equals(Object o) {
            return this == o || o instanceof ActorRef && this.refSpec.equals(o.toString());
        }

        @Override
        public final int hashCode() {
            return this.refSpec.hashCode();
        }

        @Override
        public final String toString() {
            return this.refSpec;
        }
    }

    private final class LocalActorRef implements ActorRef {
        private static final String REFSPEC_FORMAT = "actor://%s/%s/clients/%s/%s";
        private final String actorId;
        private final String refSpec;

        private LocalActorRef(String actorId) {
            this.actorId = actorId;
            this.refSpec = format(REFSPEC_FORMAT, clusterName, actorSystemName, nodeId, actorId);
        }

        @Override
        public String getActorCluster() {
            return clusterName;
        }

        @Override
        public String getActorPath() {
            return clientQueue;
        }

        @Override
        public String getActorId() {
            return actorId;
        }

        @Override
        public void tell(Object message, ActorRef sender) throws MessageDeliveryException {
            throw new UnsupportedOperationException("You cannot directly send a message to a Temp Actor inside the ActorSystemClient");
        }

        @Override
        public void tell(Object message) throws IllegalStateException, MessageDeliveryException {
            throw new UnsupportedOperationException("You cannot directly send a message to a Temp Actor inside the ActorSystemClient");
        }

        @Override
        public <T> CompletionStage<T> ask(Object message, Class<T> responseType) {
            throw new UnsupportedOperationException("You cannot directly send a message to a Temp Actor inside the ActorSystemClient");
        }

        @Override
        public <T> CompletionStage<T> ask(Object message, Class<T> responseType, Boolean persistOnResponse) {
            throw new UnsupportedOperationException("You cannot directly send a message to a Temp Actor inside the ActorSystemClient");
        }

        @Override
        public boolean isLocal() {
            return true;
        }

        @Override
        public <T> Publisher<T> publisherOf(Class<T> messageClass) {
            throw new UnsupportedOperationException("Reactive Streams protocol is not supported by the ActorSystemClient");
        }

        @Override
        public final boolean equals(Object o) {
            return this == o || o instanceof ActorRef && this.refSpec.equals(o.toString());
        }

        @Override
        public final int hashCode() {
            return this.refSpec.hashCode();
        }

        @Override
        public final String toString() {
            return this.refSpec;
        }
    }

    private static final class ClientActor implements ActorContext {
        private final ActorRef self;
        private final ElasticActor elasticActor;
        private final ActorState state;
        private final ActorSystem actorSystem;

        private ClientActor(ActorRef self, ElasticActor elasticActor, ActorState state, ActorSystem actorSystem) {
            this.self = self;
            this.elasticActor = elasticActor;
            this.state = state;
            this.actorSystem = actorSystem;
        }

        public ElasticActor getElasticActor() {
            return elasticActor;
        }

        @Override
        public ActorRef getSelf() {
            return self;
        }

        @Override
        public <T extends ActorState> T getState(Class<T> stateClass) {
            return (T) state;
        }

        @Override
        public void setState(ActorState state) {

        }

        @Override
        public ActorSystem getActorSystem() {
            return actorSystem;
        }

        @Override
        public Collection<PersistentSubscription> getSubscriptions() {
            return Collections.emptyList();
        }

        @Override
        public Map<String, Set<ActorRef>> getSubscribers() {
            return Collections.emptyMap();
        }
    }
}
