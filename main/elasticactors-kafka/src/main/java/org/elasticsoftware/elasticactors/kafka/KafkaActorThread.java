/*
 * Copyright 2013 - 2025 The Original Authors
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

package org.elasticsoftware.elasticactors.kafka;

import com.google.common.cache.Cache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import com.google.common.collect.TreeMultimap;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsoftware.elasticactors.ActorContainerRef;
import org.elasticsoftware.elasticactors.ActorNode;
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.ActorShard;
import org.elasticsoftware.elasticactors.ElasticActor;
import org.elasticsoftware.elasticactors.NodeKey;
import org.elasticsoftware.elasticactors.PhysicalNode;
import org.elasticsoftware.elasticactors.ShardKey;
import org.elasticsoftware.elasticactors.cache.EvictionListener;
import org.elasticsoftware.elasticactors.cache.NodeActorCacheManager;
import org.elasticsoftware.elasticactors.cache.ShardActorCacheManager;
import org.elasticsoftware.elasticactors.cluster.ActorRefFactory;
import org.elasticsoftware.elasticactors.cluster.ActorSystemEvent;
import org.elasticsoftware.elasticactors.cluster.ActorSystemEventListener;
import org.elasticsoftware.elasticactors.cluster.InternalActorSystem;
import org.elasticsoftware.elasticactors.cluster.ShardDistributionStrategy;
import org.elasticsoftware.elasticactors.cluster.scheduler.ScheduledMessage;
import org.elasticsoftware.elasticactors.kafka.cluster.ActorLifecycleFunction;
import org.elasticsoftware.elasticactors.kafka.cluster.ApplicationProtocol;
import org.elasticsoftware.elasticactors.kafka.cluster.ReactiveStreamsProtocol;
import org.elasticsoftware.elasticactors.kafka.serialization.KafkaActorSystemEventListenerDeserializer;
import org.elasticsoftware.elasticactors.kafka.serialization.KafkaInternalMessageDeserializer;
import org.elasticsoftware.elasticactors.kafka.serialization.KafkaInternalMessageSerializer;
import org.elasticsoftware.elasticactors.kafka.serialization.KafkaPersistentActorSerializer;
import org.elasticsoftware.elasticactors.kafka.serialization.KafkaProducerSerializer;
import org.elasticsoftware.elasticactors.kafka.serialization.KafkaScheduledMessageDeserializer;
import org.elasticsoftware.elasticactors.kafka.serialization.UUIDDeserializer;
import org.elasticsoftware.elasticactors.kafka.state.PersistentActorStore;
import org.elasticsoftware.elasticactors.kafka.state.PersistentActorStoreFactory;
import org.elasticsoftware.elasticactors.kafka.utils.TopicNamesHelper;
import org.elasticsoftware.elasticactors.messaging.DefaultInternalMessage;
import org.elasticsoftware.elasticactors.messaging.InternalMessage;
import org.elasticsoftware.elasticactors.messaging.InternalMessageFactory;
import org.elasticsoftware.elasticactors.messaging.internal.ActorNodeMessage;
import org.elasticsoftware.elasticactors.messaging.internal.CancelScheduledMessageMessage;
import org.elasticsoftware.elasticactors.messaging.internal.CreateActorMessage;
import org.elasticsoftware.elasticactors.messaging.internal.DestroyActorMessage;
import org.elasticsoftware.elasticactors.messaging.internal.PersistActorMessage;
import org.elasticsoftware.elasticactors.serialization.Deserializer;
import org.elasticsoftware.elasticactors.serialization.MessageSerializer;
import org.elasticsoftware.elasticactors.serialization.SerializationContext;
import org.elasticsoftware.elasticactors.serialization.Serializer;
import org.elasticsoftware.elasticactors.serialization.internal.ActorRefDeserializer;
import org.elasticsoftware.elasticactors.serialization.internal.InternalMessageDeserializer;
import org.elasticsoftware.elasticactors.serialization.internal.InternalMessageSerializer;
import org.elasticsoftware.elasticactors.serialization.internal.ScheduledMessageDeserializer;
import org.elasticsoftware.elasticactors.state.PersistentActor;
import org.elasticsoftware.elasticactors.tracing.Traceable;
import org.elasticsoftware.elasticactors.util.ManifestTools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.annotation.Nullable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import static org.elasticsoftware.elasticactors.kafka.utils.TopicNamesHelper.getNodeMessagesTopic;
import static org.elasticsoftware.elasticactors.util.ClassLoadingHelper.getClassHelper;
import static org.elasticsoftware.elasticactors.util.SerializationTools.deserializeMessage;

import static java.lang.String.format;

public final class KafkaActorThread extends Thread {
    private static final Logger logger = LoggerFactory.getLogger(KafkaActorSystemInstance.class);
    private static final AtomicInteger THREAD_ID_SEQUENCE = new AtomicInteger(0);
    private static final long DEFAULT_OFFSET_INCREASE = 2L;
    // this instance acts as a tombstone for stopped actors
    private static final PersistentActor<ShardKey> TOMBSTONE =
            new PersistentActor<>(null,null,null,null,null,null);
    private final KafkaConsumer<UUID, InternalMessage> messageConsumer;
    private final KafkaProducer<Object, Object> producer;
    private final KafkaConsumer<String, byte[]> stateConsumer;
    private final KafkaConsumer<String, ActorSystemEventListener> actorSystemEventListenersConsumer;
    private final KafkaConsumer<UUID, ScheduledMessage> scheduledMessagesConsumer;
    private final String clusterName;
    private final InternalActorSystem internalActorSystem;
    private final BlockingQueue<BiConsumer<KafkaConsumer<UUID, InternalMessage>, KafkaProducer<Object, Object>>> commands;
    private final Set<ShardKey> newLocalShards = new HashSet<>();
    private final Map<ShardKey, ManagedActorShard> localShards = new HashMap<>();
    private ManagedActorNode localActorNode;
    private final Map<ShardKey, KafkaActorShard> managedShards = new HashMap<>();
    private final ShardActorCacheManager shardActorCacheManager;
    private final NodeActorCacheManager nodeActorCacheManager;
    private final Serializer<PersistentActor<ShardKey>,byte[]> stateSerializer;
    private final Deserializer<byte[],PersistentActor<ShardKey>> stateDeserializer;
    private final PersistentActorStoreFactory persistentActorStoreFactory;
    private final String messagesTopic;
    private final String scheduledMessagesTopic;
    private final String actorSystemEventListenersTopic;
    private final String persistentActorsTopic;
    private boolean RUNNING = true;
    private final Integer nodeTopicPartitionId;
    private final Callback loggingCallback = (metadata, exception) -> {
        if(exception != null) {
            logger.error("Exception while sending message to KafkaProducer", exception);
        }};
    private KafkaActorSystemState state = KafkaActorSystemState.INITIALIZING;

    private enum KafkaActorSystemState {
        INITIALIZING, ACTIVE, REBALANCING
    }

    public KafkaActorThread(String clusterName,
                            String bootstrapServers,
                            String nodeId,
                            InternalActorSystem internalActorSystem,
                            ActorRefFactory actorRefFactory,
                            ShardActorCacheManager shardActorCacheManager,
                            NodeActorCacheManager nodeActorCacheManager,
                            Serializer<PersistentActor<ShardKey>, byte[]> stateSerializer,
                            Deserializer<byte[], PersistentActor<ShardKey>> stateDeserializer,
                            PersistentActorStoreFactory persistentActorStoreFactory) {
        super("KafkaActorThread-"+THREAD_ID_SEQUENCE.getAndIncrement());
        this.persistentActorStoreFactory = persistentActorStoreFactory;
        // this is the node partition that this thread will be listening on (-1 because it was already incremented)
        this.nodeTopicPartitionId = THREAD_ID_SEQUENCE.get() -1;
        this.clusterName = clusterName;
        this.internalActorSystem = internalActorSystem;
        this.shardActorCacheManager = shardActorCacheManager;
        this.nodeActorCacheManager = nodeActorCacheManager;
        this.stateSerializer = stateSerializer;
        this.stateDeserializer = stateDeserializer;
        // cache for quicker access
        this.messagesTopic = TopicNamesHelper.getMessagesTopic(internalActorSystem);
        this.scheduledMessagesTopic = TopicNamesHelper.getScheduledMessagesTopic(internalActorSystem);
        this.actorSystemEventListenersTopic = TopicNamesHelper.getActorsystemEventListenersTopic(internalActorSystem);
        this.persistentActorsTopic = TopicNamesHelper.getPersistentActorsTopic(internalActorSystem);
        final Map<String, Object> consumerConfig = new HashMap<>();
        consumerConfig.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");
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
        consumerConfig.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, IsolationLevel.READ_COMMITTED.name().toLowerCase(Locale.ROOT));
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, clusterName);
        consumerConfig.put(CommonClientConfigs.CLIENT_ID_CONFIG, nodeId + "-" + getName() + "-consumer");

        InternalMessageDeserializer internalMessageDeserializer = new InternalMessageDeserializer(new ActorRefDeserializer(actorRefFactory), internalActorSystem);
        messageConsumer = new KafkaConsumer<>(consumerConfig, new UUIDDeserializer(), new KafkaInternalMessageDeserializer(internalMessageDeserializer));

        final Map<String, Object> producerConfig = new HashMap<>();
        producerConfig.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        producerConfig.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        producerConfig.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
        producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");

        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        // add client id with stream client id prefix
        producerConfig.put(CommonClientConfigs.CLIENT_ID_CONFIG, nodeId + "-" + getName() + "-producer");
        producerConfig.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, nodeId + "-" + getName() + "-producer");

        // @todo: wrap the internal message serializer in a compressing serializer
        KafkaProducerSerializer keySerializer = new KafkaProducerSerializer(
                new KafkaInternalMessageSerializer(InternalMessageSerializer.get()),
                new KafkaPersistentActorSerializer(stateSerializer));

        KafkaProducerSerializer valueSerializer = new KafkaProducerSerializer(
                new KafkaInternalMessageSerializer(InternalMessageSerializer.get()),
                new KafkaPersistentActorSerializer(stateSerializer));

        producer = new KafkaProducer<>(producerConfig, keySerializer, valueSerializer);
        // this needs to be called once
        producer.initTransactions();

        this.commands = new LinkedBlockingQueue<>();

        final Map<String, Object> stateConsumerConfig = new HashMap<>();
        stateConsumerConfig.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1000");
        stateConsumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        stateConsumerConfig.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        stateConsumerConfig.put("internal.leave.group.on.close", false);
        stateConsumerConfig.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, Integer.toString(Integer.MAX_VALUE));
        stateConsumerConfig.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, IsolationLevel.READ_COMMITTED.name().toLowerCase(Locale.ROOT));
        stateConsumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        stateConsumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, clusterName+"-state");
        stateConsumerConfig.put(CommonClientConfigs.CLIENT_ID_CONFIG, nodeId + "-" + getName() + "-state-consumer");

        stateConsumer = new KafkaConsumer<>(stateConsumerConfig, new StringDeserializer(), new ByteArrayDeserializer());

        final Map<String, Object> scheduledMessagesConsumerConfig = new HashMap<>();
        scheduledMessagesConsumerConfig.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1000");
        scheduledMessagesConsumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        scheduledMessagesConsumerConfig.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        scheduledMessagesConsumerConfig.put("internal.leave.group.on.close", false);
        scheduledMessagesConsumerConfig.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, Integer.toString(Integer.MAX_VALUE));
        scheduledMessagesConsumerConfig.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, IsolationLevel.READ_COMMITTED.name().toLowerCase(Locale.ROOT));
        scheduledMessagesConsumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        scheduledMessagesConsumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, clusterName+"-scheduledMessages");
        scheduledMessagesConsumerConfig.put(CommonClientConfigs.CLIENT_ID_CONFIG,nodeId + "-" + getName() + "-scheduledMessages-consumer");

        KafkaScheduledMessageDeserializer scheduledMessageDeserializer
                = new KafkaScheduledMessageDeserializer(
                        new ScheduledMessageDeserializer(new ActorRefDeserializer(actorRefFactory)));
        scheduledMessagesConsumer = new KafkaConsumer<>(scheduledMessagesConsumerConfig, new UUIDDeserializer(), scheduledMessageDeserializer);

        final Map<String, Object> actorSystemEventListenersConsumerConfig = new HashMap<>();
        actorSystemEventListenersConsumerConfig.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1000");
        actorSystemEventListenersConsumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        actorSystemEventListenersConsumerConfig.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        actorSystemEventListenersConsumerConfig.put("internal.leave.group.on.close", false);
        actorSystemEventListenersConsumerConfig.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, Integer.toString(Integer.MAX_VALUE));
        actorSystemEventListenersConsumerConfig.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, IsolationLevel.READ_COMMITTED.name().toLowerCase(Locale.ROOT));
        actorSystemEventListenersConsumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        actorSystemEventListenersConsumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, clusterName+"-actorSystemEventListeners");
        actorSystemEventListenersConsumerConfig.put(CommonClientConfigs.CLIENT_ID_CONFIG, nodeId + "-" + getName() + "-actorSystemEventListeners-consumer");

        actorSystemEventListenersConsumer = new KafkaConsumer<>(actorSystemEventListenersConsumerConfig, new StringDeserializer(),
                new KafkaActorSystemEventListenerDeserializer());
    }

    @Override
    public void run() {
        BiConsumer<KafkaConsumer<UUID, InternalMessage>, KafkaProducer<Object, Object>> command;
        try {
            while (RUNNING) {
                command = pollOrWait();
                if (command != null) {
                    do {
                        command.accept(messageConsumer, producer);
                        // @todo: this could starve the other jobs in this loop as there is no stop condition
                        command = pollOrWait();
                    } while (command != null);
                }
                // a command could have changed RUNNING or switched to REBALANCING
                if (RUNNING && state == KafkaActorSystemState.ACTIVE) {
                    // consume messages
                    processMessages();
                    // consume scheduled messages
                    updateScheduledMessages();
                    // see if we need to fire any scheduled messages
                    maybeFireScheduledMessages();
                }
            }
        } catch(Exception e) {
            // @todo: we need to kill our instance somehow.. otherwise the cluster is fucked
            logger.error("FATAL: Exception in KafkaActorThread runLoop", e);
        } finally {
            // cleanup resources
            producer.close();
            stateConsumer.close();
            actorSystemEventListenersConsumer.close();
            scheduledMessagesConsumer.close();
        }
    }

    private BiConsumer<KafkaConsumer<UUID, InternalMessage>, KafkaProducer<Object, Object>> pollOrWait() {
        // handle commands, special cases when INITIALIZING or REBALANCING
        if(state == KafkaActorSystemState.ACTIVE) {
            return commands.poll(); // don't block just call
        } else {
            // while we are not active we are only handling commands, so it doesn't make sense to go into a spin loop
            try {
                return commands.poll(1, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                // noop
            }
        }
        // interrupted
        return null;
    }

    private void processMessages() {
        try {
            ConsumerRecords<UUID, InternalMessage> consumerRecords = messageConsumer.poll(Duration.ofMillis(1));
            if (!consumerRecords.isEmpty()) {
                if(logger.isDebugEnabled()) {
                    logger.debug("messageConsumer has {} records to process", consumerRecords.count());
                }
                // handle all messages in a single transaction
                producer.beginTransaction();
                // set this producer in the transactional context
                KafkaTransactionContext.setTransactionalProducer(producer);
                // the offset metadata to commit
                Map<TopicPartition, OffsetAndMetadata> offset = new HashMap<>();
                consumerRecords.partitions().forEach(topicPartition -> consumerRecords.records(topicPartition).forEach(consumerRecord -> {
                        if(logger.isDebugEnabled()) {
                            logger.debug("handling InternalMessage(sender:{}, receiver:{}, type:{})  with offset {} from topicPartition({})",
                                    consumerRecord.value().getSender(), consumerRecord.value().getReceivers().get(0),
                                    consumerRecord.value().getPayloadClass(), consumerRecord.offset(),
                                    topicPartition);
                        }
                        // handle the InternalMessage here
                        handleInternalMessage(topicPartition, consumerRecord.value());
                        // mark the message as read
                        offset.put(topicPartition, new OffsetAndMetadata(consumerRecord.offset() + 1));
                }));
                // commit the offsets
                producer.sendOffsetsToTransaction(offset, clusterName);
                // commit the transaction
                producer.commitTransaction();
            }
        } catch(WakeupException | InterruptException e) {
            logger.warn("Recoverable exception while polling for Messages", e);
        } catch(KafkaException e) {
            logger.error("FATAL: Unrecoverable exception while polling for Messages", e);
            System.exit(1);
        } catch(Throwable t) {
            logger.error("Unexpected exception while polling for Messages", t);
            System.exit(1);
        }
    }

    private void updateScheduledMessages() {
        try {
            // first see if we have new messages
            ConsumerRecords<UUID, ScheduledMessage> consumerRecords = scheduledMessagesConsumer.poll(Duration.ZERO);
            if (!consumerRecords.isEmpty()) {
                consumerRecords.partitions().forEach(topicPartition -> consumerRecords.records(topicPartition).forEach(consumerRecord -> {
                    // can be null (for deleted messages)
                    // @todo: get the shardkey from a topicmap
                    ManagedActorShard managedActorShard = this.localShards.get(new ShardKey(internalActorSystem.getName(), topicPartition.partition()));
                    if (managedActorShard != null) {
                        if (consumerRecord.value() != null) {
                            managedActorShard.scheduledMessages.put(consumerRecord.value().getFireTime(TimeUnit.MILLISECONDS), consumerRecord.value());
                        } else {
                            // for removed scheduledmessages we only have the id, we will have to search
                            managedActorShard.scheduledMessages.entries().removeIf(entry -> entry.getValue().getId().equals(consumerRecord.key()));
                        }
                    }
                }));
            }
        } catch(WakeupException | InterruptException e) {
            logger.warn("Recoverable exception while polling for ScheduledMessages", e);
        } catch(KafkaException e) {
            logger.error("FATAL: Unrecoverable exception while polling for ScheduledMessages", e);
            // @todo: this is an unrecoverable error
        } catch(Throwable t) {
            logger.error("Unexpected exception while polling for ScheduledMessages", t);
        }
    }

    private void maybeFireScheduledMessages() {
        // @todo: this needs proper error handling
        // now see if we need to fire a message
        List<ScheduledMessage> messagesToFire = this.localShards.values().stream()
                .map(managedActorShard -> managedActorShard.getScheduledMessagesThatShouldFire(System.currentTimeMillis()))
                .flatMap(List::stream).collect(Collectors.toList());
        if(!messagesToFire.isEmpty()) {
            // fire them all within a producer transaction
            try {
                producer.beginTransaction();
                messagesToFire.forEach(scheduledMessage -> {
                    // send the message (first to the shard, so it will be picked up by the normal processMessages for that shard)
                    InternalMessage internalMessage =
                        new DefaultInternalMessage(
                            scheduledMessage.getSender(),
                            scheduledMessage.getReceiver(),
                            scheduledMessage.getMessageBytes(),
                            scheduledMessage.getMessageClass().getName(),
                            scheduledMessage.getMessageQueueAffinityKey(),
                            false
                        );
                    // find out which shard to send it to (this has to be and ActorShard)
                    ShardKey destinationKey = ((ActorShard) ((ActorContainerRef) scheduledMessage.getReceiver()).getActorContainer()).getKey();
                    // and send it
                    producer.send(new ProducerRecord<>(messagesTopic, destinationKey.getShardId(), internalMessage.getId(), internalMessage));
                    // remove it from the scheduled messages topic (by setting value to null)
                    ShardKey sourceKey = ((ActorShard) ((ActorContainerRef) scheduledMessage.getSender()).getActorContainer()).getKey();
                    producer.send(new ProducerRecord<>(scheduledMessagesTopic, sourceKey.getShardId(), scheduledMessage.getId(), null));
                });
                // commit the transaction
                producer.commitTransaction();
                // now we need to remove them from the managedActorShards as well
                messagesToFire.forEach(scheduledMessage -> {
                    ShardKey sourceKey = ((ActorShard) ((ActorContainerRef) scheduledMessage.getSender()).getActorContainer()).getKey();
                    this.localShards.get(sourceKey).scheduledMessages.remove(scheduledMessage.getFireTime(TimeUnit.MILLISECONDS), scheduledMessage);
                });
            } catch(RetriableException e) {
                logger.warn("Recoverable exception while sending ScheduledMessages", e);
            } catch(ProducerFencedException e) {
                logger.error("FATAL: ProducerFenced while committing transaction, another Node seems to be handling the same shards", e);
                // @todo: this is an unrecoverable error
            } catch(KafkaException e) {
                logger.error("FATAL: Unrecoverable exception while committing producer transaction", e);
                // @todo: this is an unrecoverable error
            } catch(Throwable t) {
                logger.error("Unexpected exception while processing ScheduledMessages", t);
            }
        }
    }

    void send(ShardKey shard, InternalMessage internalMessage) {
        ProducerRecord<Object, Object> producerRecord =
                new ProducerRecord<>(messagesTopic, shard.getShardId(), internalMessage.getId(), internalMessage);
        doSend(producerRecord, KafkaTransactionContext.getProducer());
    }

    private void doSend(ProducerRecord<Object, Object> producerRecord, KafkaProducer<Object, Object> transactionalProducer) {
        if(transactionalProducer == null) {
            // no transaction so hand over to the current thread (and send in it's own transaction)
            runCommand((kafkaConsumer, kafkaProducer) -> {
                try {
                    kafkaProducer.beginTransaction();
                    kafkaProducer.send(producerRecord, loggingCallback);
                    kafkaProducer.commitTransaction();
                } catch(RetriableException e) {
                    logger.warn("Recoverable exception while sending ProducerRecord", e);
                } catch(ProducerFencedException e) {
                    logger.error("FATAL: ProducerFenced while committing transaction, another Node seems to be handling the same shards", e);
                    // @todo: this is an unrecoverable error
                } catch(KafkaException e) {
                    logger.error("FATAL: Unrecoverable exception while committing producer transaction", e);
                    // @todo: this is an unrecoverable error
                } catch(Throwable t) {
                    logger.error("Unexpected exception while sending ProducerRecord", t);
                }
            });
        } else {
            transactionalProducer.send(producerRecord, loggingCallback);
        }
    }

    void send(NodeKey node, int partition, InternalMessage internalMessage) {
        ProducerRecord<Object, Object> producerRecord =
                new ProducerRecord<>(getNodeMessagesTopic(internalActorSystem, node.getNodeId()), partition, internalMessage.getId(), internalMessage);
        doSend(producerRecord, KafkaTransactionContext.getProducer());
    }

    void schedule(ShardKey shard, ScheduledMessage scheduledMessage) {
        ProducerRecord<Object, Object> producerRecord =
                new ProducerRecord<>(scheduledMessagesTopic, shard.getShardId(), scheduledMessage.getId(), scheduledMessage);
        doSend(producerRecord, KafkaTransactionContext.getProducer());
    }

    void register(ShardKey shard, ActorSystemEvent event, ActorSystemEventListener listener) {
        ProducerRecord<Object, Object> producerRecord =
            new ProducerRecord<>(actorSystemEventListenersTopic, shard.getShardId(),
                    format("%s:%s", event.name(), listener.getActorId()), listener);
        doSend(producerRecord, KafkaTransactionContext.getProducer());
    }

    void deregister(ShardKey shard, ActorSystemEvent event, ActorRef listener) {
        ProducerRecord<Object, Object> producerRecord =
                new ProducerRecord<>(actorSystemEventListenersTopic, shard.getShardId(),
                        format("%s:%s", event.name(), listener.getActorId()), null);
        doSend(producerRecord, KafkaTransactionContext.getProducer());
    }

    void assign(KafkaActorNode node, boolean primary) {
        // we are registering the local node, which means this ActorThread is managing the node topic
        runCommand((kafkaConsumer, kafkaProducer) ->
                this.localActorNode = new ManagedActorNode(node, primary));
    }

    void assign(KafkaActorShard actorShard) {
        runCommand((kafkaConsumer, kafkaProducer) -> this.managedShards.put(actorShard.getKey(), actorShard));
    }

    void stopRunning() {
        runCommand((kafkaConsumer, kafkaProducer) -> this.RUNNING = false);
    }

    CompletionStage<Boolean> prepareRebalance(Multimap<PhysicalNode, ShardKey> shardDistribution,
                                              ShardDistributionStrategy distributionStrategy) {
        final CompletableFuture<Boolean> completableFuture = new CompletableFuture<>();
        runCommand((kafkaConsumer, kafkaProducer) -> {
            // this is to determine whether the system is stable or not
            final AtomicBoolean stable = new AtomicBoolean(true);
            // switch state to rebalancing
            this.state = KafkaActorSystemState.REBALANCING;
            // filter only on shards the are managed by this thread
            shardDistribution.forEach((node, shardKey) -> {
                        // find the actorShard (will be null if not managed by this instance)
                        KafkaActorShard actorShard = managedShards.get(shardKey);
                        if(actorShard != null) {
                            // see if the assigned node is the local node
                            if (node.isLocal()) {
                                if (actorShard.getOwningNode() == null || !actorShard.getOwningNode().equals(node)) {
                                    String owningNodeId = actorShard.getOwningNode() != null ? actorShard.getOwningNode().getId() : "<No Node>";
                                    logger.info("I will own {}", shardKey);
                                    try {
                                        // register with the strategy to wait for shard to be released
                                        distributionStrategy.registerWaitForRelease(actorShard, node);
                                    } catch (Exception e) {
                                        logger.error("IMPORTANT: waiting on release of shard {} from node {} failed,  ElasticActors cluster is unstable. Please check all nodes", shardKey, owningNodeId, e);
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
                                    logger.info("I already own {}", shardKey);
                                }
                            } else {
                                // the shard will be managed by another node
                                if (actorShard.getOwningNode() == null || actorShard.getOwningNode().isLocal()) {
                                    logger.info("{} will own {}", node, shardKey);
                                    try {
                                        // destroy the current local shard instance
                                        if (actorShard.getOwningNode() != null) {
                                            // register the new node
                                            actorShard.setOwningNode(node);
                                            // and remove from the managed local shards
                                            this.localShards.remove(shardKey).destroy();
                                            // now we can release the shard to the other node
                                            distributionStrategy.signalRelease(actorShard, node);
                                        }
                                    } catch (Exception e) {
                                        logger.error("IMPORTANT: signalling release of shard {} to node {} failed, ElasticActors cluster is unstable. Please check all nodes", shardKey, node, e);
                                        // signal this back later
                                        stable.set(false);
                                    }
                                } else {
                                    // shard was already remote
                                    logger.info("{} will own {}", node, shardKey);
                                }
                            }
                        }
                    });
            // we are done, signal back to stable flag
            completableFuture.complete(stable.get());
            // we stay in the rebalancing state as we need to perform the rebalance
        });
        return completableFuture;
    }

    CompletionStage<Set<Integer>> performRebalance() {
        final CompletableFuture<Set<Integer>> completableFuture = new CompletableFuture<>();
        runCommand((kafkaConsumer, kafkaProducer) -> {
            List<ManagedActorShard> newManagedShards = new LinkedList<>();
            try {
                // we need to process the new shards and start owning them
                this.newLocalShards.forEach(shardKey -> {
                    // we need to create the state store
                    PersistentActorStore actorStore = createStateStore(shardKey);
                    // promote to local shard
                    ManagedActorShard managedActorShard = new ManagedActorShard(managedShards.get(shardKey), actorStore);
                    localShards.put(shardKey, managedActorShard);
                    // we need to do initialization after the assignment
                    newManagedShards.add(managedActorShard);
                });
                // clear the newLocalShards for the next rebalance op
                this.newLocalShards.clear();
                // assign all the correct partitions
                assignPartitions();
                if(!newManagedShards.isEmpty()) {
                    // now we need to initialize the state stores for all new shards
                    initializeStateStores(newManagedShards);
                    // and the scheduled messages
                    initializeScheduledMessages(newManagedShards);
                    // and run any actorsystem event listeners
                    initializeAndRunActorSystemEventListeners(newManagedShards);
                }
                // switch the state to ACTIVE
                this.state = KafkaActorSystemState.ACTIVE;
                // and signal success
                completableFuture.complete(newLocalShards.stream()
                        .map(ShardKey::getShardId)
                        .collect(Collectors.toSet()));
            } catch(Exception e) {
                logger.error("FATAL Exception on performRebalance", e);
                // @todo: this should signal some kind of fatal exception
                completableFuture.completeExceptionally(e);
            }

        });
        return completableFuture;
    }

    private void assignPartitions() {
        // assign the message partitions
        List<TopicPartition> messagePartitions = this.localShards.keySet().stream()
                .map(managedActorShard -> new TopicPartition(messagesTopic, managedActorShard.getShardId())).collect(Collectors.toList());
        if(localActorNode != null) {
            // node topics have exactly the number of partitions as there are KafkaActorThreads per node
            // this is very fickle but needed to support the affinityKey logic for TempActors
            messagePartitions.add(new TopicPartition(getNodeMessagesTopic(internalActorSystem, localActorNode.actorNode.getKey().getNodeId()), nodeTopicPartitionId));
        }
        this.messageConsumer.assign(messagePartitions);

        // also need to assign the state partitions
        List<TopicPartition> statePartitions = this.localShards.keySet().stream()
                .map(managedActorShard -> new TopicPartition(persistentActorsTopic, managedActorShard.getShardId())).collect(Collectors.toList());
        this.stateConsumer.assign(statePartitions);

        // and the scheduled messages
        List<TopicPartition> scheduledMessagesPartitions = this.localShards.keySet().stream()
                .map(managedActorShard -> new TopicPartition(scheduledMessagesTopic, managedActorShard.getShardId())).collect(Collectors.toList());
        this.scheduledMessagesConsumer.assign(scheduledMessagesPartitions);

        // the actorsystem event listeners
        List<TopicPartition> actorSystemEventListenersPartitions =
                this.localShards.keySet().stream()
                .map(managedActorShard -> new TopicPartition(actorSystemEventListenersTopic, managedActorShard.getShardId())).collect(Collectors.toList());
        this.actorSystemEventListenersConsumer.assign(actorSystemEventListenersPartitions);
    }

    private PersistentActorStore createStateStore(ShardKey shardKey) {
        return persistentActorStoreFactory.create(shardKey, stateDeserializer);
    }

    private void initializeStateStores(List<ManagedActorShard> managedActorShards) {
        // in the case of a scale out there are no new managed shards
        if(managedActorShards.isEmpty()) {
            return;
        }
        // loop over the state consumer until nothing is left
        // seek to the beginning for the new actor shards
        // we don't need to do this if we don't commit anything (autocommit is off for the stateConsumer)
        List<TopicPartition> topicPartitions = managedActorShards.stream()
                .map(managedActorShard -> new TopicPartition(persistentActorsTopic, managedActorShard.getKey().getShardId()))
                .collect(Collectors.toList());
        // seek to the end for all (the ones we already own)
        stateConsumer.seekToEnd(stateConsumer.assignment());
        // and then to the beginning for the new ones only
        stateConsumer.seekToBeginning(topicPartitions);
        // this is to optimize the lookup in the poll loop
        Map<Integer, ManagedActorShard> partitionsToShards = managedActorShards.stream()
                .collect(Collectors.toMap(managedActorShard -> managedActorShard.getKey().getShardId(), managedActorShard -> managedActorShard));
        // we need to be sure to read the topics fully. stopping to early will cause loss of state!
        // topic is always the same so we just index on the partition here
        final Map<Integer, Long> endOffsets = stateConsumer.endOffsets(topicPartitions).entrySet()
                .stream().filter(e -> e.getValue() > 0).collect(Collectors.toMap(e -> e.getKey().partition(), Map.Entry::getValue));
        // find the right position to start reading from
        partitionsToShards.forEach((key, value) -> {
            if(value.actorStore.getOffset() >= 0) {
                stateConsumer.seek(new TopicPartition(persistentActorsTopic, value.getKey().getShardId()), value.actorStore.getOffset()+1);
                // see if we are at the end of the partition already
                Long endOffset = endOffsets.get(value.getKey().getShardId());
                if(endOffset != null && (endOffset - DEFAULT_OFFSET_INCREASE) == value.actorStore.getOffset()) {
                    endOffsets.remove(value.getKey().getShardId());
                }
            }
        });

        // and poll till you can't poll no more
        ConsumerRecords<String, byte[]> stateRecords = null;
        int totalCount = 0;
        do {
            try {
                stateRecords = stateConsumer.poll(10);
                totalCount += stateRecords.count();
                // distribute the data to the stores
                stateRecords.iterator().forEachRemaining(consumerRecord -> {
                    // value can be null (if actor was stopped and state deleted
                    if(consumerRecord.value() != null) {
                        partitionsToShards.get(consumerRecord.partition()).actorStore.put(consumerRecord.key(), consumerRecord.value(), consumerRecord.offset());
                    }
                    // see if we still have seen (at least) everything for this partition
                    Long endOffset = endOffsets.get(consumerRecord.partition());
                    if(endOffset != null && (endOffset - DEFAULT_OFFSET_INCREASE) == consumerRecord.offset()) {
                        endOffsets.remove(consumerRecord.partition());
                    }
                });
            } catch(WakeupException | InterruptException e) {
                logger.warn("Recoverable exception while polling PersistentActors state", e);
                // @todo: find out how to handle this
            } catch(KafkaException e) {
                logger.error("FATAL: Unrecoverable exception while polling PersistentActors state", e);
                // @todo: this is an unrecoverable error
            } catch(Throwable t) {
                logger.error("Unexpected exception while populating PersistentActorStores", t);
            }
        } while((stateRecords != null && !stateRecords.isEmpty()) || !endOffsets.isEmpty());
        int uniques = managedActorShards.stream().mapToInt(value -> value.actorStore.count()).sum();
        logger.info("Loaded {} unique persistent actors from {} entries", uniques, totalCount);
    }

    private void initializeScheduledMessages(List<ManagedActorShard> managedActorShards) {
        // on scale out the managed actorshards can be empty
        if(managedActorShards.isEmpty()) {
            return;
        }
        List<TopicPartition> topicPartitions = managedActorShards.stream()
                .map(managedActorShard -> new TopicPartition(scheduledMessagesTopic, managedActorShard.getKey().getShardId()))
                .collect(Collectors.toList());
        // seek to the beginning for the new actor shards
        scheduledMessagesConsumer.seekToBeginning(topicPartitions);
        // this is to optimize the lookup in the poll loop
        Map<Integer, ManagedActorShard> partitionsToShards = managedActorShards.stream()
                .collect(Collectors.toMap(managedActorShard -> managedActorShard.getKey().getShardId(), managedActorShard -> managedActorShard));
        // see if we have any messages at all
        final Map<Integer, Long> endOffsets = scheduledMessagesConsumer.endOffsets(topicPartitions).entrySet()
                .stream().filter(e -> e.getValue() > 0).collect(Collectors.toMap(e -> e.getKey().partition(), Map.Entry::getValue));
        // and poll till you can't poll no more
        ConsumerRecords<UUID, ScheduledMessage> scheduleMessageRecords = null;
        do {
            try {
                scheduleMessageRecords = scheduledMessagesConsumer.poll(10);
                // distribute the data to the scheduledMessages maps
                scheduleMessageRecords.iterator().forEachRemaining(consumerRecord -> {
                    // value can be null if the scheduled message was deleted
                    if(consumerRecord.value() != null) {
                        partitionsToShards.get(consumerRecord.partition())
                                .scheduledMessages.put(consumerRecord.value().getFireTime(TimeUnit.MILLISECONDS),
                                consumerRecord.value());
                    } else {
                        // could be that we need to remove one that was already added
                        partitionsToShards.get(consumerRecord.partition()).scheduledMessages.entries()
                                .removeIf(entry -> entry.getValue().getId().equals(consumerRecord.key()));
                    }
                    // see if we still have seen (at least) everything for this partition
                    Long endOffset = endOffsets.get(consumerRecord.partition());
                    if(endOffset != null && (endOffset - DEFAULT_OFFSET_INCREASE) == consumerRecord.offset()) {
                        endOffsets.remove(consumerRecord.partition());
                    }
                });
            } catch(WakeupException | InterruptException e) {
                logger.warn("Recoverable exception while polling ScheduledMessages state", e);
                // @todo: find out how to handle this
            } catch(KafkaException e) {
                logger.error("FATAL: Unrecoverable exception while polling ScheduledMessages state", e);
                // @todo: this is an unrecoverable error
            } catch(Throwable t) {
                logger.error("Unexpected exception while populating ScheduledMessage", t);
            }
        } while((scheduleMessageRecords != null && !scheduleMessageRecords.isEmpty()) || !endOffsets.isEmpty());
        //} while(scheduleMessageRecords != null && !scheduleMessageRecords.isEmpty());
        // make sure we commit here so that we don't get replays later when we poll for more messages
        try {
            scheduledMessagesConsumer.commitSync();
        } catch(WakeupException | InterruptException e) {
            logger.warn("Recoverable exception calling commitSync on scheduledMessagesConsumer", e);
        } catch(KafkaException e) {
            logger.error("FATAL: Unrecoverable exception calling commitSync on scheduledMessagesConsumer", e);
        }
    }

    private void initializeAndRunActorSystemEventListeners(List<ManagedActorShard> managedActorShards) {
        // sanity check
        if(managedActorShards.isEmpty()) {
            return;
        }
        List<TopicPartition> topicPartitions = managedActorShards.stream()
                .map(managedActorShard -> new TopicPartition(actorSystemEventListenersTopic, managedActorShard.getKey().getShardId()))
                .collect(Collectors.toList());
        // seek to the beginning for the new actor shards
        actorSystemEventListenersConsumer.seekToBeginning(topicPartitions);
        // this is to optimize the lookup in the poll loop
        Map<Integer, ManagedActorShard> partitionsToShards = managedActorShards.stream()
                .collect(Collectors.toMap(managedActorShard -> managedActorShard.getKey().getShardId(), managedActorShard -> managedActorShard));
        // see if we have any messages so we don't miss any
        // @todo: this will contain the minimum amount of messages to handle (can be more)
        final Map<Integer, Long> endOffsets = actorSystemEventListenersConsumer
                .endOffsets(topicPartitions).entrySet()
                .stream().filter(e -> e.getValue() > 0)
                .collect(Collectors.toMap(e -> e.getKey().partition(), Map.Entry::getValue));
        // and poll till you can't poll no more
        ConsumerRecords<String, ActorSystemEventListener> consumerRecords = null;
        do {
            try {
                // @todo: potentially just send these as messages
                consumerRecords = actorSystemEventListenersConsumer.poll(10);
                // run the logic for each actor
                consumerRecords.iterator().forEachRemaining(consumerRecord -> {
                    ActorSystemEventListener eventListener = consumerRecord.value();
                    // eventlistener can be null (when it was deleted)
                    if(eventListener != null) {
                        ManagedActorShard managedActorShard = partitionsToShards.get(consumerRecord.partition());
                        ActorRef receiverRef = internalActorSystem.actorFor(eventListener.getActorId());
                        PersistentActor<ShardKey> persistentActor = managedActorShard.getPersistentActor(receiverRef);
                        InternalMessage internalMessage = new DefaultInternalMessage(
                            null,
                            receiverRef,
                            eventListener.getMessageBytes(),
                            eventListener.getMessageClass().getName(),
                            eventListener.getMessageQueueAffinityKey(),
                            false
                        );
                        // start a new transaction for each message
                        producer.beginTransaction();
                        try {
                            // set this producer in the transactional context
                            KafkaTransactionContext.setTransactionalProducer(producer);
                            doInActorContext(ApplicationProtocol::handleMessage, managedActorShard, persistentActor, internalMessage);
                            producer.commitTransaction();
                        } catch(ProducerFencedException e) {
                            logger.error("FATAL: ProducerFenced while committing transaction, another Node seems to be handling the same shards", e);
                        } catch(KafkaException e) {
                            logger.error("FATAL: Unrecoverable exception while comitting producer transaction", e);
                            // @todo: this is an unrecoverable error
                        } catch(Throwable t) {
                            logger.error("Unexpected exception while generating ActorSystemEventListener message", t);
                        } finally {
                            KafkaTransactionContext.clear();
                        }
                    }
                    // see if we still have seen (at least) everything for this partition
                    Long endOffset = endOffsets.get(consumerRecord.partition());
                    if(endOffset != null && (endOffset - DEFAULT_OFFSET_INCREASE) == consumerRecord.offset()) {
                        endOffsets.remove(consumerRecord.partition());
                    }
                });
            } catch(WakeupException | InterruptException e) {
                logger.warn("Recoverable exception while polling ActorSystemEventListeners state", e);
                // @todo: find out how to handle this
            } catch(KafkaException e) {
                logger.error("FATAL: Unrecoverable exception while polling ActorSystemEventListeners state", e);
                // @todo: this is an unrecoverable error
            }
        } while((consumerRecords != null && !consumerRecords.isEmpty()) || !endOffsets.isEmpty());

    }

    Integer getNodeTopicPartitionId() {
        return nodeTopicPartitionId;
    }

    void createTempActor(ActorRef ref, CreateActorMessage createMessage) {
        runCommand((kafkaConsumer, kafkaProducer) -> {
            try {
                createActor(localActorNode, createMessage, ref, null);
            } catch(Exception e) {
                logger.error("Exception while creating TempActor", e);
            }
        });
    }

    CompletableFuture<Void> initializeServiceActors() {
        final CompletableFuture<Void> completableFuture = new CompletableFuture<>();
        runCommand((kafkaConsumer, kafkaProducer) -> {
            Set<String> serviceActors = internalActorSystem.getConfiguration().getServices();
            if (serviceActors != null && !serviceActors.isEmpty()) {
                // initialize the service actors in the context
                serviceActors.forEach(s -> {
                    ActorRef serviceRef = internalActorSystem.serviceActorFor(s);
                    ElasticActor serviceActor = internalActorSystem.getConfiguration().getService(s);
                    // because shards are initialized before service actors it could be possible the service actor
                    // received a message already an did Just In Time activating
                    if(!this.localActorNode.initializedActors.contains(serviceRef)) {
                        InternalActorContext.setContext(new ServiceActorContext(serviceRef, internalActorSystem));
                        try {
                            serviceActor.postActivate(null);
                        } catch (Exception e) {
                            // @todo: send an error message to the sender
                            logger.error("Exception while handling message for service [{}]", serviceRef, e);
                        } finally {
                            InternalActorContext.clearContext();
                            this.localActorNode.initializedActors.add(serviceRef);
                        }
                    }
                });

            }
            completableFuture.complete(null);
        });
        return completableFuture;
    }

    private void runCommand(BiConsumer<KafkaConsumer<UUID, InternalMessage>, KafkaProducer<Object, Object>> command) {
        this.commands.offer(command);
    }

    private void handleInternalMessage(TopicPartition topicPartition, InternalMessage im) {
        // shard message
        if(topicPartition.topic().equals(messagesTopic)) {
            handleInternalMessage(this.localShards.get(new ShardKey(internalActorSystem.getName(), topicPartition.partition())), im);
        } else { // node message
            handleInternalMessage(this.localActorNode, im);
        }
    }

    private void handleInternalMessage(
        ManagedActorContainer managedActorContainer,
        InternalMessage internalMessage)
    {
        // assumption here is that all receivers are for the same shard
        internalMessage.getReceivers().forEach(actorRef -> {
            if(actorRef.getActorId() != null) {
                handleActorMessage(managedActorContainer, actorRef, internalMessage);
            } else {
                handleContainerMessage(managedActorContainer, internalMessage);
            }
        });
    }

    private void handleActorMessage(ManagedActorContainer managedActorContainer, ActorRef receiverRef, InternalMessage internalMessage) {
        PersistentActor actor = managedActorContainer.getPersistentActor(receiverRef);
        if(TOMBSTONE == actor) {
            // actor doesn't exist (either never did or was recently destroyed)
            sendUndeliverable(internalMessage, receiverRef);
        } else if(actor == null) {
            // it could be a service actor (in that case the container is an ActorNode)
            // see if it is a service
            ElasticActor serviceInstance = internalActorSystem.getServiceInstance(receiverRef);
            if(serviceInstance != null) {
                // this can only be for a ManagedActorNode
                boolean jitActivationNeeded = ((ManagedActorNode)managedActorContainer).initializedActors.contains(receiverRef);
                InternalActorContext.setContext(new ServiceActorContext(receiverRef, internalActorSystem));
                try {
                    if(jitActivationNeeded) {
                        ((ManagedActorNode)managedActorContainer).initializedActors.add(receiverRef);
                        serviceInstance.postActivate(null);
                    }
                    Object message = deserializeMessage(internalActorSystem, internalMessage);
                    if(internalMessage.isUndeliverable()) {
                        serviceInstance.onUndeliverable(internalMessage.getSender(), message);
                    } else {
                        serviceInstance.onReceive(internalMessage.getSender(), message);
                    }
                } catch(Exception e) {
                    // @todo: send an error message to the sender
                    logger.error("Exception while handling message for service [{}]",receiverRef,e);
                } finally {
                    InternalActorContext.clearContext();
                }
            } else {
                sendUndeliverable(internalMessage, receiverRef);
            }
        } else {
            // we have a receiving actor, find the concrete ElasticActor code
            if(internalMessage.isUndeliverable()) {
                if(internalMessage.getPayloadClass().startsWith("org.elasticsoftware.elasticactors.messaging.reactivestreams")) {
                    doInActorContext(ReactiveStreamsProtocol::handleUndeliverableMessage, managedActorContainer, actor, internalMessage);
                } else {
                    doInActorContext(ApplicationProtocol::handleUndeliverableMessage, managedActorContainer, actor, internalMessage);
                }
            } else {
                if(internalMessage.getPayloadClass().startsWith("org.elasticsoftware.elasticactors.messaging.reactivestreams")) {
                    doInActorContext(ReactiveStreamsProtocol::handleMessage, managedActorContainer, actor, internalMessage);
                } else {
                    doInActorContext(ApplicationProtocol::handleMessage, managedActorContainer, actor, internalMessage);
                }
            }
        }
    }

    private void handleContainerMessage(ManagedActorContainer managedActorContainer, InternalMessage internalMessage) {
        try {
            Object message = deserializeMessage(internalActorSystem, internalMessage);
            // check if the actor exists
            if (message instanceof CreateActorMessage) {
                CreateActorMessage createActorMessage = (CreateActorMessage) message;
                ActorRef ref = internalActorSystem.actorFor(createActorMessage.getActorId());
                if (!managedActorContainer.containsKey(ref)) {
                    createActor(managedActorContainer, createActorMessage, ref, internalMessage);
                } else {
                    // we need to load the actor since we need to run the postActivate logic
                    managedActorContainer.getPersistentActor(ref);
                }
            } else if (message instanceof DestroyActorMessage) {
                DestroyActorMessage destroyActorMessage = (DestroyActorMessage) message;
                destroyActor(managedActorContainer, destroyActorMessage, internalMessage);
            } else if (message instanceof CancelScheduledMessageMessage) {
                CancelScheduledMessageMessage cancelMessage = (CancelScheduledMessageMessage) message;
                // this will only ever happen for a Shard
                cancelScheduledMessage((ManagedActorShard) managedActorContainer, cancelMessage);
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
                        // we currently don't handle message undeliverable for ActorNodeMessages
                        logger.error("ActorNode with id [{}] is not reachable, discarding message of type [{}] from [{}] for [{}]",
                                actorNodeMessage.getNodeId(), actorNodeMessage.getMessage().getClass().getName(), internalMessage.getSender(),
                                actorNodeMessage.getReceiverRef());
                    }
                } else {
                    // we currently don't handle message undeliverable for ActorNodeMessages
                    logger.error("undeliverable ActorNodeMessages are currently not supported");
                }
            } else if(message instanceof PersistActorMessage) {
                PersistActorMessage persistMessage = (PersistActorMessage) message;
                persistActor(managedActorContainer, persistMessage.getActorRef());
            }
        } catch (Exception e) {
            // @todo: determine if this is a recoverable error case or just a programming error
            logger.error("Exception while handling InternalMessage for Shard [{}]; senderRef [{}], messageType [{}]",
                    managedActorContainer.getKey(),
                    internalMessage.getSender(), internalMessage.getPayloadClass(), e);
        }
    }

    private void doInActorContext(ActorLifecycleFunction handler,
                                  ManagedActorContainer managedActorContainer,
                                  PersistentActor persistentActor,
                                  InternalMessage internalMessage) {
        // setup the context
        InternalActorContext.setContext(persistentActor);
        boolean shouldUpdateState = false;
        ElasticActor receiver = internalActorSystem.getActorInstance(persistentActor.getSelf(), persistentActor.getActorClass());
        try {
            shouldUpdateState = handler.apply(internalActorSystem, persistentActor, receiver, persistentActor.getSelf(), internalMessage);
        } catch (Exception e) {
            logger.error("Exception in doInActorContext",e);
        } finally {
            // reset the serialization context
            SerializationContext.reset();
            // clear the state from the thread
            InternalActorContext.clearContext();
        }
        if(shouldUpdateState) {
            managedActorContainer.persistActor(persistentActor);
        }
    }

    private void sendUndeliverable(InternalMessage internalMessage, ActorRef receiverRef) {
        // if a message-undeliverable is undeliverable, don't send an undeliverable message back!
        ActorRef senderRef = internalMessage.getSender();
        try {
            if (senderRef instanceof ActorContainerRef && !internalMessage.isUndeliverable()) {
                ((ActorContainerRef) senderRef).getActorContainer().undeliverableMessage(internalMessage, receiverRef);
            } else if(internalMessage.isUndeliverable()) {
                logger.error("Receiver for undeliverable message not found: message type '{}' , receiver '{}'", internalMessage.getPayloadClass(), receiverRef);
            } else {
                logger.warn("Could not send message undeliverable: original message type '{}' , receiver '{}'", internalMessage.getPayloadClass(), receiverRef);
            }
        } catch(Exception e) {
            logger.error("Exception while sending undeliverable message", e);
        }
    }

    private void createActor(ManagedActorContainer managedActorContainer,
                             CreateActorMessage createMessage,
                             ActorRef ref,
                             InternalMessage internalMessage) throws ClassNotFoundException {
        final Class<? extends ElasticActor> actorClass =
            (Class<? extends ElasticActor>) getClassHelper().forName(createMessage.getActorClass());
        final String actorStateVersion = ManifestTools.extractActorStateVersion(actorClass);
        PersistentActor<?> persistentActor =
                new PersistentActor<>(managedActorContainer.getKey(), internalActorSystem, actorStateVersion, ref,
                        createMessage.getAffinityKey(), actorClass, createMessage.getInitialState());
        // add it to the actorCache
        managedActorContainer.getActorCache().put(ref, persistentActor);
        // persist it to the actor store (if any)
        managedActorContainer.persistActor(persistentActor);

        doInActorContext(ApplicationProtocol::createActor, managedActorContainer, persistentActor, internalMessage);
    }

    private void destroyActor(ManagedActorContainer managedActorShard, DestroyActorMessage destroyMessage, InternalMessage internalMessage) {
        PersistentActor<?> persistentActor = managedActorShard.getPersistentActor(destroyMessage.getActorRef());
        if(persistentActor != null) {
            // put a tombstone in the cache
            managedActorShard.getActorCache().put(persistentActor.getSelf(), TOMBSTONE);
            // remove from the store
            managedActorShard.deleteActor(persistentActor);
            doInActorContext(ApplicationProtocol::destroyActor, managedActorShard, persistentActor, internalMessage);
        }
    }

    private void persistActor(ManagedActorContainer managedActorShard, ActorRef actorRef) {
        PersistentActor<?> persistentActor = managedActorShard.getPersistentActor(actorRef);
        // make sure we're not dealing with a tombstone
        if(persistentActor != null && !(persistentActor == TOMBSTONE)) {
            managedActorShard.persistActor(persistentActor);
        }
    }

    private void cancelScheduledMessage(ManagedActorShard managedActorShard, CancelScheduledMessageMessage cancelMessage) {
        ProducerRecord<Object, Object> producerRecord =
                new ProducerRecord<>(scheduledMessagesTopic, managedActorShard.getKey().getShardId(),
                        cancelMessage.getMessageId(), null);
        // update the stored state (this will run in the current transaction)
        doSend(producerRecord, KafkaTransactionContext.getProducer());
    }

    private InternalMessage createInternalMessage(ActorRef from, List<? extends ActorRef> to, Object message) throws IOException {
        MessageSerializer<Object> messageSerializer = (MessageSerializer<Object>) internalActorSystem.getSerializer(message.getClass());
        return InternalMessageFactory.createWithSerializedPayload(
            from,
            to,
            messageSerializer,
            message
        );
    }

    private final class ManagedActorShard implements EvictionListener<PersistentActor<ShardKey>>, ManagedActorContainer<ShardKey> {
        private final KafkaActorShard actorShard;
        private final Cache<ActorRef,PersistentActor<ShardKey>> actorCache;
        private final PersistentActorStore actorStore;
        private final TreeMultimap<Long, ScheduledMessage> scheduledMessages;

        public ManagedActorShard(KafkaActorShard actorShard,
                                 PersistentActorStore actorStore) {
            this.actorShard = actorShard;
            this.actorCache = shardActorCacheManager.create(actorShard.getKey(), this);
            this.actorStore = actorStore;
            this.scheduledMessages = TreeMultimap.create(Comparator.naturalOrder(), Comparator.naturalOrder());
        }

        @Override
        public ShardKey getKey() {
            return actorShard.getKey();
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
            actorStore.destroy();
        }

        @Override
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
                } else {
                    return null;
                }
            } else {
                return persistentActor;
            }
        }

        public boolean actorExists(ActorRef actorRef) {
            return actorStore.containsKey(actorRef.getActorId());
        }

        public List<ScheduledMessage> getScheduledMessagesThatShouldFire(long now) {
            return this.scheduledMessages.values().stream()
                    .filter(scheduledMessage -> scheduledMessage.getFireTime(TimeUnit.MILLISECONDS) < now)
                    .collect(Collectors.toList());
        }

        @Override
        public void persistActor(PersistentActor<ShardKey> persistentActor) {
            // write to the producer (this will be within the current transaction)
            try {
                byte[] serializedActor = stateSerializer.serialize(persistentActor);
                if(logger.isDebugEnabled()) {
                    logger.debug("Serializing PersistentActor: keySize={}, valueSize={}",
                            persistentActor.getSelf().getActorId().getBytes(StandardCharsets.UTF_8).length,
                            serializedActor.length);
                }
                ProducerRecord<Object,Object> producerRecord = new ProducerRecord<>(persistentActorsTopic, this.getKey().getShardId(),
                        persistentActor.getSelf().getActorId(), serializedActor);
                // this will always be within a transaction
                producer.send(producerRecord, (metadata, exception) -> {
                    if(metadata != null) {
                        if(metadata.hasOffset()) {
                            if(this.actorStore.isConcurrent()) {
                                this.actorStore.put(persistentActor.getSelf().getActorId(), serializedActor, metadata.offset());
                            } else {
                                // need to update on the KafkaActorThread
                                runCommand((kafkaConsumer, kafkaProducer) ->
                                        this.actorStore.put(persistentActor.getSelf().getActorId(), serializedActor, metadata.offset()));
                            }
                        } else {
                            // update without updating the offset
                            this.actorStore.put(persistentActor.getSelf().getActorId(), serializedActor);
                        }
                    } else {
                        logger.error("Exception while sending message to KafkaProducer", exception);
                    }
                });
            } catch(IOException e) {
                // throw the same exception that would have been thrown by the Kafka serializer
                throw new SerializationException(format("Exception while serializing state for actor %s", persistentActor.getSelf().getActorId()), e);
            }
        }

        @Override
        public void deleteActor(PersistentActor<ShardKey> persistentActor) {
            this.actorStore.remove(persistentActor.getSelf().getActorId());
            // and from the underlying topic
            // @todo: this will lead to inconsistent state when the transaction fails
            ProducerRecord<Object,Object> producerRecord = new ProducerRecord<>(persistentActorsTopic, this.getKey().getShardId(),
                    persistentActor.getSelf().getActorId(), null);
            doSend(producerRecord, KafkaTransactionContext.getProducer());
        }

        @Override
        public boolean containsKey(ActorRef actorRef) {
            return actorExists(actorRef);
        }

        @Override
        public Cache<ActorRef, PersistentActor<ShardKey>> getActorCache() {
            return actorCache;
        }
    }

    private final class ManagedActorNode implements EvictionListener<PersistentActor<NodeKey>>, ManagedActorContainer<NodeKey> {
        private final KafkaActorNode actorNode;
        private final boolean primary;
        private final Cache<ActorRef,PersistentActor<NodeKey>> actorCache;
        private final Set<ActorRef> initializedActors = new HashSet<>();

        private ManagedActorNode(KafkaActorNode actorNode, boolean primary) {
            this.actorNode = actorNode;
            this.actorCache = nodeActorCacheManager.create(actorNode.getKey(), this);
            this.primary = primary;
        }

        public boolean isPrimary() {
            return primary;
        }

        @Override
        public void onEvicted(PersistentActor<NodeKey> value) {
            Traceable traceable = value.getState() instanceof Traceable
                ? (Traceable) value.getState()
                : null;
            boolean hasTraceData = traceable != null
                && (traceable.getTraceContext() != null || traceable.getCreationContext() != null);
            logger.error(
                "CRITICAL WARNING: Actor [{}] of type [{}] got evicted from the cache. "
                    + "This can lead to issues using temporary actors. "
                    + "Please increase the maximum size of the node actor cache "
                    + "by using the 'ea.nodeCache.maximumSize' property."
                    + "{}"
                    + "{}"
                    + "{}",
                value.getSelf(),
                value.getActorClass().getName(),
                hasTraceData
                    ? " Temporary Actor created with the following contexts in scope:"
                    : "",
                hasTraceData
                    ? toLoggableString(traceable.getCreationContext())
                    : "",
                hasTraceData
                    ? toLoggableString(traceable.getTraceContext())
                    : ""
            );
        }

        private String toLoggableString(@Nullable Object object) {
            return object != null
                ? " " + object + "."
                : "";
        }

        @Override
        public PersistentActor<NodeKey> getPersistentActor(ActorRef actorRef) {
            return actorCache.getIfPresent(actorRef);
        }

        @Override
        public void persistActor(PersistentActor<NodeKey> persistentActor) {
            // noop
        }

        @Override
        public void deleteActor(PersistentActor<NodeKey> persistentActor) {
            // remove from cache
            actorCache.invalidate(persistentActor.getSelf());
        }

        @Override
        public boolean containsKey(ActorRef actorRef) {
            return actorCache.getIfPresent(actorRef) != null;
        }

        @Override
        public NodeKey getKey() {
            return actorNode.getKey();
        }

        @Override
        public Cache<ActorRef, PersistentActor<NodeKey>> getActorCache() {
            return actorCache;
        }
    }

}
