package org.elasticsoftware.elasticactors.kafka;

import com.google.common.cache.Cache;
import com.google.common.collect.ImmutableList;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsoftware.elasticactors.*;
import org.elasticsoftware.elasticactors.cache.EvictionListener;
import org.elasticsoftware.elasticactors.cache.NodeActorCacheManager;
import org.elasticsoftware.elasticactors.cache.ShardActorCacheManager;
import org.elasticsoftware.elasticactors.cluster.ActorRefFactory;
import org.elasticsoftware.elasticactors.cluster.InternalActorSystem;
import org.elasticsoftware.elasticactors.cluster.ShardDistributionStrategy;
import org.elasticsoftware.elasticactors.cluster.scheduler.ScheduledMessageKey;
import org.elasticsoftware.elasticactors.cluster.scheduler.SchedulerService;
import org.elasticsoftware.elasticactors.kafka.cluster.ActorLifecycleFunction;
import org.elasticsoftware.elasticactors.kafka.cluster.ApplicationProtocol;
import org.elasticsoftware.elasticactors.kafka.cluster.ReactiveStreamsProtocol;
import org.elasticsoftware.elasticactors.kafka.serialization.*;
import org.elasticsoftware.elasticactors.kafka.state.InMemoryPersistentActorStore;
import org.elasticsoftware.elasticactors.kafka.state.PersistentActorStore;
import org.elasticsoftware.elasticactors.messaging.InternalMessage;
import org.elasticsoftware.elasticactors.messaging.InternalMessageImpl;
import org.elasticsoftware.elasticactors.messaging.internal.*;
import org.elasticsoftware.elasticactors.serialization.*;
import org.elasticsoftware.elasticactors.serialization.internal.ActorRefDeserializer;
import org.elasticsoftware.elasticactors.serialization.internal.InternalMessageDeserializer;
import org.elasticsoftware.elasticactors.serialization.internal.InternalMessageSerializer;
import org.elasticsoftware.elasticactors.state.PersistentActor;
import org.elasticsoftware.elasticactors.util.ManifestTools;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static org.apache.kafka.common.requests.IsolationLevel.READ_COMMITTED;
import static org.elasticsoftware.elasticactors.util.SerializationTools.deserializeMessage;

public class KafkaActorThread extends Thread {
    private static final Logger logger = LogManager.getLogger(KafkaActorSystemInstance.class);
    private static final AtomicInteger THREAD_ID_SEQUENCE = new AtomicInteger(1);
    // this instance acts as a tombstone for stopped actors
    private static final PersistentActor<ShardKey> TOMBSTONE = new PersistentActor<>(null,null,null,null,null,null);
    private final KafkaConsumer<UUID, InternalMessage> messageConsumer;
    private final KafkaProducer<Object, Object> producer;
    private final KafkaConsumer<String, byte[]> stateConsumer;
    private final String clusterName;
    private final InternalActorSystem internalActorSystem;
    private final ActorRefFactory actorRefFactory;
    private final BlockingQueue<BiConsumer<KafkaConsumer<UUID, InternalMessage>, KafkaProducer<Object, Object>>> commands;
    private final Set<ShardKey> newLocalShards = new HashSet<>();
    private final Map<ShardKey, ManagedActorShard> localShards = new HashMap<>();
    private ManagedActorNode localActorNode;
    private final Map<ShardKey, KafkaActorShard> managedShards = new HashMap<>();
    private final SchedulerService scheduler;
    private final ShardActorCacheManager shardActorCacheManager;
    private final NodeActorCacheManager nodeActorCacheManager;
    private final Serializer<PersistentActor<ShardKey>,byte[]> stateSerializer;
    private final Deserializer<byte[],PersistentActor<ShardKey>> stareDeserializer;

    public KafkaActorThread(String clusterName,
                            String bootstrapServers,
                            InternalActorSystem internalActorSystem,
                            ActorRefFactory actorRefFactory,
                            ShardActorCacheManager shardActorCacheManager,
                            NodeActorCacheManager nodeActorCacheManager,
                            Serializer<PersistentActor<ShardKey>, byte[]> stateSerializer,
                            Deserializer<byte[], PersistentActor<ShardKey>> stareDeserializer) {
        super("KafkaActorThread-"+THREAD_ID_SEQUENCE.getAndIncrement());
        this.clusterName = clusterName;
        this.internalActorSystem = internalActorSystem;
        this.actorRefFactory = actorRefFactory;
        // @todo: create some internal scheduler implementation
        this.scheduler = null;
        this.shardActorCacheManager = shardActorCacheManager;
        this.nodeActorCacheManager = nodeActorCacheManager;
        this.stateSerializer = stateSerializer;
        this.stareDeserializer = stareDeserializer;
        final Map<String, Object> consumerConfig = new HashMap<>();
        consumerConfig.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "50");
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerConfig.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumerConfig.put("internal.leave.group.on.close", false);
        // MAX_POLL_INTERVAL_MS_CONFIG needs to be large for streams to handle cases when
        // streams is recovering data from state stores. We may set it to Integer.MAX_VALUE since
        // the streams code itself catches most exceptions and acts accordingly without needing
        // this timeout. Note however that deadlocks are not detected (by definition) so we
        // are losing the ability to detect them by setting this value to large. Hopefully
        // deadlocks happen very rarely or never.
        consumerConfig.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, Integer.toString(Integer.MAX_VALUE));
        consumerConfig.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, READ_COMMITTED.name().toLowerCase(Locale.ROOT));
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, clusterName);
        consumerConfig.put(CommonClientConfigs.CLIENT_ID_CONFIG,internalActorSystem.getNode().getKey().getNodeId() + "-" + getName() + "-consumer");

        InternalMessageDeserializer internalMessageDeserializer = new InternalMessageDeserializer(new ActorRefDeserializer(actorRefFactory), internalActorSystem);
        messageConsumer = new KafkaConsumer<>(consumerConfig, new UUIDDeserializer(), new KafkaInternalMessageDeserializer(internalMessageDeserializer));

        final Map<String, Object> producerConfig = new HashMap<>();
        producerConfig.put(ProducerConfig.LINGER_MS_CONFIG, "100");
        producerConfig.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        producerConfig.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        producerConfig.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
        producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");

        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        // add client id with stream client id prefix
        producerConfig.put(CommonClientConfigs.CLIENT_ID_CONFIG, internalActorSystem.getNode().getKey().getNodeId() + "-" + getName() + "-producer");
        producerConfig.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, internalActorSystem.getNode().getKey().getNodeId() + "-" + getName() + "-producer");

        // @todo: wrap the internal message serializer in a compressing serializer
        KafkaProducerSerializer serializer = new KafkaProducerSerializer(
                new KafkaInternalMessageSerializer(InternalMessageSerializer.get()),
                new KafkaPersistentActorSerializer(stateSerializer));

        producer = new KafkaProducer<>(producerConfig, serializer, serializer);
        // this needs to be called once
        producer.initTransactions();

        this.commands = new LinkedBlockingQueue<>();

        final Map<String, Object> stateConsumerConfig = new HashMap<>();
        stateConsumerConfig.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");
        stateConsumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        stateConsumerConfig.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        stateConsumerConfig.put("internal.leave.group.on.close", false);
        // MAX_POLL_INTERVAL_MS_CONFIG needs to be large for streams to handle cases when
        // streams is recovering data from state stores. We may set it to Integer.MAX_VALUE since
        // the streams code itself catches most exceptions and acts accordingly without needing
        // this timeout. Note however that deadlocks are not detected (by definition) so we
        // are losing the ability to detect them by setting this value to large. Hopefully
        // deadlocks happen very rarely or never.
        stateConsumerConfig.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, Integer.toString(Integer.MAX_VALUE));
        stateConsumerConfig.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, READ_COMMITTED.name().toLowerCase(Locale.ROOT));
        stateConsumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        stateConsumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, clusterName+"-state");
        stateConsumerConfig.put(CommonClientConfigs.CLIENT_ID_CONFIG,internalActorSystem.getNode().getKey().getNodeId() + "-" + getName() + "-state-consumer");

        stateConsumer = new KafkaConsumer<>(stateConsumerConfig, new StringDeserializer(), new ByteArrayDeserializer());
    }

    @Override
    public void run() {
        // handle commands
        BiConsumer<KafkaConsumer<UUID, InternalMessage>, KafkaProducer<Object, Object>> command = commands.poll();
        if(command != null) {
            do {
                command.accept(messageConsumer, producer);
                command = commands.poll();
            } while(command != null);
        }
        // consume
        ConsumerRecords<UUID, InternalMessage> consumerRecords = messageConsumer.poll(1);
        if(!consumerRecords.isEmpty()) {
            consumerRecords.partitions().forEach(topicPartition -> consumerRecords.records(topicPartition).forEach(consumerRecord -> {
                // start a new transaction for each message
                producer.beginTransaction();
                // handle the InternalMessage here
                handleInternalMessage(topicPartition, consumerRecord.value());
                // mark the message as read
                Map<TopicPartition, OffsetAndMetadata> offset = new HashMap<>();
                offset.put(topicPartition, new OffsetAndMetadata(consumerRecord.offset() + 1));
                // commit the offset
                producer.sendOffsetsToTransaction(offset, clusterName);
                // commit the transaction
                producer.commitTransaction();
            }));
        }

    }

    void send(ShardKey shard, InternalMessage internalMessage) {
        runCommand((kafkaConsumer, kafkaProducer) -> {
            ProducerRecord<Object, Object> producerRecord =
                    new ProducerRecord<>(clusterName, shard.getShardId(), internalMessage.getId(), internalMessage);
            kafkaProducer.send(producerRecord, (metadata, exception) -> {
                // @todo: message sending failed. what to do now?
            });
        });
    }

    void send(NodeKey node, InternalMessage internalMessage) {
        runCommand((kafkaConsumer, kafkaProducer) -> {
            ProducerRecord<Object, Object> producerRecord =
                    new ProducerRecord<>(generateNodeTopic(node) , internalMessage.getId(), internalMessage);
            kafkaProducer.send(producerRecord, (metadata, exception) -> {
                // @todo: message sending failed. what to do now?
            });
        });
    }

    void assign(KafkaActorNode node) {
        runCommand((kafkaConsumer, kafkaProducer) -> {
            // we are registering the local node, which means this ActorThread is managing the node topic
            this.localActorNode = new ManagedActorNode(node);
        });
    }

    void assign(KafkaActorShard actorShard) {
        runCommand((kafkaConsumer, kafkaProducer) -> this.managedShards.put(actorShard.getKey(), actorShard));
    }

    CompletionStage<Boolean> prepareRebalance(Map<PhysicalNode, ShardKey> shardDistribution,
                                              ShardDistributionStrategy distributionStrategy) {
        final CompletableFuture<Boolean> completableFuture = new CompletableFuture<>();
        runCommand((kafkaConsumer, kafkaProducer) -> {
            final AtomicBoolean stable = new AtomicBoolean(true);
            // filter only on shards the are managed by this thread
            shardDistribution.entrySet().stream().filter(entry -> managedShards.keySet().contains(entry.getValue()))
                    .forEach(entry -> {
                        // find the actorShard
                        KafkaActorShard actorShard = managedShards.get(entry.getValue());
                        // more convenient names
                        ShardKey shardKey = entry.getValue();
                        PhysicalNode node = entry.getKey();
                        // see if the assigned node is the local node
                        if(node.isLocal()) {
                            if(actorShard.getOwningNode() == null || !actorShard.getOwningNode().equals(node)) {
                                String owningNodeId = actorShard.getOwningNode() != null ? actorShard.getOwningNode().getId() : "<No Node>";
                                logger.info(format("I will own %s", shardKey.toString()));
                                try {
                                    // register with the strategy to wait for shard to be released
                                    distributionStrategy.registerWaitForRelease(actorShard, node);
                                } catch(Exception e) {
                                    logger.error(format("IMPORTANT: waiting on release of shard %s from node %s failed,  ElasticActors cluster is unstable. Please check all nodes", shardKey, owningNodeId), e);
                                    // signal this back later
                                    stable.set(false);
                                } finally {
                                    // register the new owner
                                    actorShard.setOwningNode(node);
                                    // register as a new local shard (i.e. to start consuming later)
                                    this.newLocalShards.add(shardKey);
                                    // in the performRebalance step this shard will be promoted to a managed shard
                                }
                            } else {
                                // we own the shard already, no change needed
                                logger.info(format("I already own %s", shardKey.toString()));
                            }
                        } else {
                            // the shard will be managed by another node
                            if (actorShard.getOwningNode() == null || actorShard.getOwningNode().isLocal()) {
                                logger.info(format("%s will own %s", node, shardKey));
                                try {
                                    // destroy the current local shard instance
                                    if (actorShard.getOwningNode() != null) {
                                        // stop owning the scheduler shard
                                        // @todo: implement scheduler compacted topic
                                        scheduler.unregisterShard(shardKey);
                                        // register the new node
                                        actorShard.setOwningNode(node);
                                        // and remove from the managed local shards
                                        this.localShards.remove(shardKey).destroy();
                                        // now we can release the shard to the other node
                                        distributionStrategy.signalRelease(actorShard, node);
                                    }
                                } catch(Exception e) {
                                    logger.error(format("IMPORTANT: signalling release of shard %s to node %s failed, ElasticActors cluster is unstable. Please check all nodes", shardKey, node), e);
                                    // signal this back later
                                    stable.set(false);
                                }
                            } else {
                                // shard was already remote
                                logger.info(format("%s will own %s", node, shardKey));
                            }
                        }
                    });
            // we are done, signal back to stable flag
            completableFuture.complete(stable.get());

        });
        return completableFuture;
    }

    CompletionStage<Integer> performRebalance() {
        final CompletableFuture<Integer> completableFuture = new CompletableFuture<>();
        runCommand((kafkaConsumer, kafkaProducer) -> {
            try {
                // we need to process the new shards and start owning them
                this.newLocalShards.forEach(shardKey -> {
                    // we need to initialize the state stores
                    PersistentActorStore actorStore = initializeStateStore(shardKey);
                    // start owning the scheduler shard (this will start sending messages)
                    scheduler.registerShard(shardKey);
                    // promote to local shard
                    localShards.put(shardKey, new ManagedActorShard(managedShards.get(shardKey), actorStore));
                });
                // and signal success
                completableFuture.complete(newLocalShards.size());
            } catch(Exception e) {
                logger.error("FATAL Exception on performRebalance", e);
                // @todo: this should signal some kind of fatal exception
                completableFuture.completeExceptionally(e);
            } finally {
                // clear the newLocalShards for the next rebalance op
                this.newLocalShards.clear();
                // @todo: do we need to run with the shards that we know on error?
                assignPartitions();
            }

        });
        return completableFuture;
    }

    void createTempActor(CreateActorMessage createActorMessage) {
        runCommand((kafkaConsumer, kafkaProducer) -> {
            // @todo: run the create TempActor logic
        });
    }

    void handleTempActorMessage(PersistentActor<NodeKey> tempActor, InternalMessage internalMessage) {
        runCommand((kafkaConsumer, kafkaProducer) -> {
            // @todo: handle message for temp actor, we need to to support the affinityKey logic (i.e. execute the temp
            // @todo: logic in the same thread as the calling actor
        });
    }

    private void runCommand(BiConsumer<KafkaConsumer<UUID, InternalMessage>, KafkaProducer<Object, Object>> command) {
        this.commands.offer(command);
    }

    private void handleInternalMessage(TopicPartition topicPartition, InternalMessage im) {
        // shard message
        if(topicPartition.topic().equals(clusterName)) {
            handleInternalMessage(this.localShards.get(new ShardKey(internalActorSystem.getName(), topicPartition.partition())), im);
        } else {
            // node message
        }
    }

    private void handleInternalMessage(ManagedActorShard managedActorShard, InternalMessage im) {
        // assumption here is that all receivers are for the same shard
        boolean needsCopy = im.getReceivers().size() > 1;
        im.getReceivers().forEach(actorRef -> {
            InternalMessage internalMessage = (needsCopy) ? im.copyOf() : im;
            if(actorRef.getActorId() != null) {
                handleActorMessage(managedActorShard, actorRef, internalMessage);
            } else {
                handleShardMessage(managedActorShard, internalMessage);
            }
        });
    }

    private void handleActorMessage(ManagedActorShard managedActorShard, ActorRef receiverRef, InternalMessage internalMessage) {
        PersistentActor<ShardKey> actor = managedActorShard.getPersistentActor(receiverRef);
        if(actor == null || TOMBSTONE == actor) {
            // actor doesn't exist (either never did or was recently destroyed)
            sendUndeliverable(internalMessage, receiverRef);
        } else {
            // we have a receiving actor, find the concrete ElasticActor code
            if(internalMessage.isUndeliverable()) {
                if(internalMessage.getPayloadClass().startsWith("org.elasticsoftware.elasticactors.messaging.reactivestreams")) {
                    doInActorContext(ReactiveStreamsProtocol::handleUndeliverableMessage, managedActorShard, actor, internalMessage);
                } else {
                    doInActorContext(ApplicationProtocol::handleUndeliverableMessage, managedActorShard, actor, internalMessage);
                }
            } else {
                if(internalMessage.getPayloadClass().startsWith("org.elasticsoftware.elasticactors.messaging.reactivestreams")) {
                    doInActorContext(ReactiveStreamsProtocol::handleMessage, managedActorShard, actor, internalMessage);
                } else {
                    doInActorContext(ApplicationProtocol::handleMessage, managedActorShard, actor, internalMessage);
                }
            }
        }
    }

    private void handleShardMessage(ManagedActorShard managedActorShard, InternalMessage internalMessage) {
        try {
            Object message = deserializeMessage(internalActorSystem, internalMessage);
            // check if the actor exists
            if (message instanceof CreateActorMessage) {
                CreateActorMessage createActorMessage = (CreateActorMessage) message;
                if (!managedActorShard.actorStore.containsKey(createActorMessage.getActorId())) {
                    createActor(managedActorShard, createActorMessage, internalMessage);
                } else {
                    // we need to activate the actor since we need to run the postActivate logic
                    PersistentActor<ShardKey> persistentActor =
                            managedActorShard.getPersistentActor(internalActorSystem.actorFor(createActorMessage.getActorId()));
                    doInActorContext(ApplicationProtocol::activateActor, managedActorShard, persistentActor, internalMessage);
                }
            } else if (message instanceof DestroyActorMessage) {
                DestroyActorMessage destroyActorMessage = (DestroyActorMessage) message;
                destroyActor(managedActorShard, destroyActorMessage, internalMessage);
            } else if (message instanceof CancelScheduledMessageMessage) {
                CancelScheduledMessageMessage cancelMessage = (CancelScheduledMessageMessage) message;
                // @todo: this won't work like this
                internalActorSystem.getInternalScheduler().cancel(managedActorShard.actorShard.getKey(), new ScheduledMessageKey(cancelMessage.getMessageId(), cancelMessage.getFireTime()));
            } else if(message instanceof ActorNodeMessage) {
                if(!internalMessage.isUndeliverable()) {
                    ActorNodeMessage actorNodeMessage = (ActorNodeMessage) message;
                    ActorNode actorNode = internalActorSystem.getNode(actorNodeMessage.getNodeId());
                    // can be null if the node is not active
                    if (actorNode != null) {
                        if(!actorNodeMessage.isUndeliverable()) {
                            actorNode.sendMessage(internalMessage.getSender(), actorNodeMessage.getReceiverRef(), actorNodeMessage.getMessage());
                        } else {
                            // we need to recreate the InternalMessage first, otherwise the undeliverable logic
                            // won't work
                            InternalMessage originalMessage = createInternalMessage(actorNodeMessage.getReceiverRef(), ImmutableList.of(internalMessage.getSender()), actorNodeMessage.getMessage());
                            actorNode.undeliverableMessage(originalMessage, internalMessage.getSender());
                        }
                    } else {
                        // @todo: we currently don't handle message undeliverable for ActorNodeMessages
                        logger.error(String.format("ActorNode with id [%s] is not reachable, discarding message of type [%s] from [%s] for [%s]",
                                actorNodeMessage.getNodeId(), actorNodeMessage.getMessage().getClass().getName(), internalMessage.getSender(),
                                actorNodeMessage.getReceiverRef()));
                    }
                } else {
                    // @todo: we currently don't handle message undeliverable for ActorNodeMessages
                    logger.error("undeliverable ActorNodeMessages are currently not supported");
                }
            } else if(message instanceof PersistActorMessage) {
                PersistActorMessage persistMessage = (PersistActorMessage) message;
                persistActor(managedActorShard, persistMessage.getActorRef());
            }
        } catch (Exception e) {
            // @todo: determine if this is a recoverable error case or just a programming error
            logger.error(String.format("Exception while handling InternalMessage for Shard [%s]; senderRef [%s], messageType [%s]",
                    managedActorShard.actorShard.getKey().toString(),
                    internalMessage.getSender().toString(), internalMessage.getPayloadClass()), e);
        }
    }

    private void doInActorContext(ActorLifecycleFunction handler,
                                  ManagedActorShard managedActorShard,
                                  PersistentActor persistentActor,
                                  InternalMessage internalMessage) {
        // setup the context
        Exception executionException = null;
        InternalActorContext.setContext(persistentActor);
        SerializationContext.initialize();
        boolean shouldUpdateState = false;
        ElasticActor receiver = internalActorSystem.getActorInstance(persistentActor.getSelf(), persistentActor.getActorClass());
        try {
            shouldUpdateState = handler.apply(internalActorSystem, persistentActor, receiver, persistentActor.getSelf(), internalMessage);
        } catch (Exception e) {
            logger.error("Exception in doInActorContext",e);
            executionException = e;
        } finally {
            // reset the serialization context
            SerializationContext.reset();
            // clear the state from the thread
            InternalActorContext.getAndClearContext();
        }
        // @todo see if we need to store here or if it's better to delegate this upwards
        if(shouldUpdateState) {
            persistActor(managedActorShard, persistentActor);
        }
    }

    private void sendUndeliverable(InternalMessage internalMessage, ActorRef receiverRef) {
        // if a message-undeliverable is undeliverable, don't send an undeliverable message back!
        ActorRef senderRef = internalMessage.getSender();
        try {
            if (senderRef != null && senderRef instanceof ActorContainerRef && !internalMessage.isUndeliverable()) {
                ((ActorContainerRef) senderRef).getActorContainer().undeliverableMessage(internalMessage, receiverRef);
            } else if(internalMessage.isUndeliverable()) {
                logger.error(format("Receiver for undeliverable message not found: message type '%s' , receiver '%s'", internalMessage.getPayloadClass(), receiverRef.toString()));
            } else {
                logger.warn(format("Could not send message undeliverable: original message type '%s' , receiver '%s'", internalMessage.getPayloadClass(), receiverRef.toString()));
            }
        } catch(Exception e) {
            logger.error("Exception while sending undeliverable message", e);
        }
    }

    private void createActor(ManagedActorShard managedActorShard, CreateActorMessage createMessage, InternalMessage internalMessage) throws ClassNotFoundException {
        ActorRef ref = internalActorSystem.actorFor(createMessage.getActorId());
        final Class<? extends ElasticActor> actorClass = (Class<? extends ElasticActor>) Class.forName(createMessage.getActorClass());
        final String actorStateVersion = ManifestTools.extractActorStateVersion(actorClass);
        PersistentActor<ShardKey> persistentActor =
                new PersistentActor<>(managedActorShard.actorShard.getKey(), internalActorSystem, actorStateVersion, ref, actorClass, createMessage.getInitialState());

        // @todo: create the actor here

        doInActorContext(ApplicationProtocol::createActor, managedActorShard, persistentActor, internalMessage);
    }

    private void destroyActor(ManagedActorShard managedActorShard, DestroyActorMessage destroyMessage, InternalMessage internalMessage) {
        PersistentActor<ShardKey> persistentActor = managedActorShard.getPersistentActor(destroyMessage.getActorRef());
        if(persistentActor != null) {
            // put a tombstone in the cache
            managedActorShard.actorCache.put(persistentActor.getSelf(), TOMBSTONE);
            // @todo: remove from the underlying store

            doInActorContext(ApplicationProtocol::destroyActor, managedActorShard, persistentActor, internalMessage);
        }
    }

    private void persistActor(ManagedActorShard managedActorShard, ActorRef actorRef) {
        PersistentActor<ShardKey> persistentActor = managedActorShard.getPersistentActor(actorRef);
        // make sure we're not dealing with a tombstone
        if(persistentActor != null && !(persistentActor == TOMBSTONE)) {
            persistActor(managedActorShard, persistentActor);
        }
    }

    private void persistActor(ManagedActorShard managedActorShard, PersistentActor<ShardKey> persistentActor) {
        // @todo: write the actor state to the backing store here
    }

    private InternalMessage createInternalMessage(ActorRef from, List<? extends ActorRef> to, Object message) throws IOException {
        MessageSerializer<Object> messageSerializer = (MessageSerializer<Object>) internalActorSystem.getSerializer(message.getClass());
        if(messageSerializer == null) {
            logger.error(String.format("No message serializer found for class: %s. NOT sending message",
                    message.getClass().getSimpleName()));
            return null;
        }
        // get the durable flag
        Message messageAnnotation = message.getClass().getAnnotation(Message.class);
        final boolean durable = (messageAnnotation != null) && messageAnnotation.durable();
        final int timeout = (messageAnnotation != null) ? messageAnnotation.timeout() : Message.NO_TIMEOUT;
        return new InternalMessageImpl(from, ImmutableList.copyOf(to), SerializationContext.serialize(messageSerializer, message),message.getClass().getName(),durable, timeout);
    }

    private void assignPartitions() {
        List<TopicPartition> topicPartitions = this.localShards.entrySet().stream()
                .map(entry -> new TopicPartition(clusterName, entry.getKey().getShardId())).collect(Collectors.toList());
        if(localActorNode != null) {
            // node topics have only 1 partition (for now)
            topicPartitions.add(new TopicPartition(generateNodeTopic(localActorNode.actorNode.getKey()), 0));
        }
        this.messageConsumer.assign(topicPartitions);
    }

    private PersistentActorStore initializeStateStore(ShardKey shardKey) {
        // @todo: load the state here
        return new InMemoryPersistentActorStore(stareDeserializer);
    }


    private String generateNodeTopic(NodeKey node) {
        return format("%s.nodes.%s", clusterName, node.getNodeId());
    }


    private final class ManagedActorShard implements EvictionListener<PersistentActor<ShardKey>> {
        private final KafkaActorShard actorShard;
        private final Cache<ActorRef,PersistentActor<ShardKey>> actorCache;
        private final PersistentActorStore actorStore;

        public ManagedActorShard(KafkaActorShard actorShard,
                                 PersistentActorStore actorStore) {
            this.actorShard = actorShard;
            this.actorCache = shardActorCacheManager.create(actorShard.getKey(), this);
            this.actorStore = actorStore;
        }

        @Override
        public void onEvicted(PersistentActor<ShardKey> value) {
            // this should only be called in the context of the current KafkaActorThread
            // see if it is not a tombstone that gets evicted
            if(!(TOMBSTONE == value)) {
                // run the passivate logic
                doInActorContext(ApplicationProtocol::passivateActor, this, value, null);
            }
        }

        public void destroy() {
            // delete the cache (this will run the onEvicted logic)
            shardActorCacheManager.destroy(actorCache);
            // @todo: maybe we need to destroy the state store here?
        }

        public PersistentActor<ShardKey> getPersistentActor(ActorRef actorRef) {
            PersistentActor<ShardKey> persistentActor = actorCache.getIfPresent(actorRef);
            if(persistentActor == null) {
                // materialize from actor store
                persistentActor = actorStore.getPersistentActor(actorRef.getActorId());
                if(persistentActor != null) {
                    // actor exists, we need to run the activate logic and then cache it
                    doInActorContext(ApplicationProtocol::activateActor, this, persistentActor, null);
                    actorCache.put(actorRef, persistentActor);
                    return persistentActor;
                }
            }
            // actor doesn't exist.. simply return null to signal that fact
            return null;
        }

        public boolean actorExists(ActorRef actorRef) {
            return actorStore.containsKey(actorRef.getActorId());
        }

    }

    private final class ManagedActorNode implements EvictionListener<PersistentActor<NodeKey>> {
        private final KafkaActorNode actorNode;
        private final Cache<ActorRef,PersistentActor<NodeKey>> actorCache;

        private ManagedActorNode(KafkaActorNode actorNode) {
            this.actorNode = actorNode;
            this.actorCache = nodeActorCacheManager.create(actorNode.getKey(), this);
        }

        @Override
        public void onEvicted(PersistentActor<NodeKey> value) {
            // this is a TempActor being evicted
        }
    }

}
