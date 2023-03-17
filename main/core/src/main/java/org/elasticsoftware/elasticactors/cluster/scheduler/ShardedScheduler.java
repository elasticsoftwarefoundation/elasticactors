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

package org.elasticsoftware.elasticactors.cluster.scheduler;

import io.micrometer.core.instrument.binder.jvm.ExecutorServiceMetrics;
import jakarta.inject.Inject;
import org.elasticsoftware.elasticactors.ActorContainer;
import org.elasticsoftware.elasticactors.ActorContainerRef;
import org.elasticsoftware.elasticactors.ActorContextHolder;
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.ActorShard;
import org.elasticsoftware.elasticactors.MessageDeliveryException;
import org.elasticsoftware.elasticactors.ShardKey;
import org.elasticsoftware.elasticactors.cluster.InternalActorSystem;
import org.elasticsoftware.elasticactors.cluster.InternalActorSystems;
import org.elasticsoftware.elasticactors.cluster.metrics.MicrometerConfiguration;
import org.elasticsoftware.elasticactors.messaging.ScheduledMessageImpl;
import org.elasticsoftware.elasticactors.scheduler.ScheduledMessageRef;
import org.elasticsoftware.elasticactors.serialization.MessageDeserializer;
import org.elasticsoftware.elasticactors.serialization.MessageSerializer;
import org.elasticsoftware.elasticactors.tracing.MessagingContextManager.MessagingScope;
import org.elasticsoftware.elasticactors.util.ByteBufferUtils;
import org.elasticsoftware.elasticactors.util.concurrent.DaemonThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static org.elasticsoftware.elasticactors.tracing.CreationContext.forScheduling;
import static org.elasticsoftware.elasticactors.tracing.MessagingContextManager.getManager;

import static java.lang.String.format;

/**
 * @author Joost van de Wijgerd
 */
public final class ShardedScheduler implements SchedulerService,ScheduledMessageRefFactory {
    private static final Logger logger = LoggerFactory.getLogger(ShardedScheduler.class);
    private ScheduledMessageRepository scheduledMessageRepository;
    private InternalActorSystem actorSystem;
    private ScheduledExecutorService scheduledExecutorService;
    private final ConcurrentHashMap<ShardKey, ConcurrentHashMap<ScheduledMessageKey, ScheduledFuture<?>>> scheduledFutures = new ConcurrentHashMap<>();
    private final int numberOfWorkers;
    private final MicrometerConfiguration micrometerConfiguration;

    @PostConstruct
    public synchronized void init() {
        logger.info("Initializing Sharded Cluster Scheduler with {} worker threads", numberOfWorkers);
        ScheduledExecutorService scheduler =
            Executors.newScheduledThreadPool(numberOfWorkers, new DaemonThreadFactory("SCHEDULER"));
        if (micrometerConfiguration != null) {
            scheduledExecutorService = ExecutorServiceMetrics.monitor(
                micrometerConfiguration.getRegistry(),
                scheduler,
                micrometerConfiguration.getComponentName(),
                micrometerConfiguration.getMetricPrefix(),
                micrometerConfiguration.getTags()
            );
        } else {
            scheduledExecutorService = scheduler;
        }
    }

    @PreDestroy
    public void destroy() {
        logger.info("Destroying Sharded Cluster Scheduler");
        scheduledExecutorService.shutdownNow();
    }

    public ShardedScheduler() {
        this(Runtime.getRuntime().availableProcessors(), null);
    }

    public ShardedScheduler(int numberOfWorkers, @Nullable
        MicrometerConfiguration micrometerConfiguration) {
        this.numberOfWorkers = numberOfWorkers;
        this.micrometerConfiguration = micrometerConfiguration;
    }

    @Inject
    public void setScheduledMessageRepository(ScheduledMessageRepository scheduledMessageRepository) {
        this.scheduledMessageRepository = scheduledMessageRepository;
    }

    @Inject
    public void setActorSystem(InternalActorSystem actorSystem) {
        this.actorSystem = actorSystem;
    }

    private void schedule(ShardKey shardKey, ScheduledMessage scheduledMessage) {
        /*
         * Using computeIfPresent here allows for more granular locking instead of locking at the
         * scheduler or at the map level
         */
        ConcurrentHashMap<ScheduledMessageKey, ScheduledFuture<?>> currentScheduledFuturesForShard =
                scheduledFutures.computeIfPresent(shardKey, (key, scheduledFuturesForShard) -> {
                    // Schedule only if not scheduled before
                    scheduledFuturesForShard.computeIfAbsent(
                            scheduledMessage.getKey(),
                            messageKey -> scheduledExecutorService.schedule(
                                    new ScheduledMessageRunnable(
                                            key,
                                            scheduledMessage),
                                    scheduledMessage.getDelay(TimeUnit.MILLISECONDS),
                                    TimeUnit.MILLISECONDS));
                    return scheduledFuturesForShard;
                });
        if (currentScheduledFuturesForShard == null) {
            throw new RejectedExecutionException(format(
                    "Shard: %s is not registered, please call registerShard first",
                    shardKey));
        }
    }

    private void unschedule(ShardKey shardKey, ScheduledMessageKey messageKey) {
        /*
         * Using computeIfPresent here allows for more granular locking instead of locking at the
         * scheduler or at the map level
         */
        ConcurrentHashMap<ScheduledMessageKey, ScheduledFuture<?>> currentScheduledFuturesForShard =
                scheduledFutures.computeIfPresent(shardKey, (key, scheduledFuturesForShard) -> {
                    ScheduledFuture<?> scheduledFuture =
                            scheduledFuturesForShard.remove(messageKey);
                    if (scheduledFuture != null) {
                        scheduledFuture.cancel(false);
                    }
                    return scheduledFuturesForShard;
                });
        if (currentScheduledFuturesForShard == null) {
            throw new IllegalArgumentException(format(
                    "Shard: %s is not registered, please call registerShard first",
                    shardKey));
        }
    }

    @Override
    public void registerShard(ShardKey shardKey) {
        // obtain the scheduler shard
        // Using computeIfAbsent so we can lock the map propertly while loading the messages
        scheduledFutures.computeIfAbsent(shardKey, key -> {
            // fetch block from repository
            // @todo: for now we'll fetch all, this obviously has memory issues
            long startTime = logger.isInfoEnabled() ? System.nanoTime() : 0L;
            List<ScheduledMessage> scheduledMessages = scheduledMessageRepository.getAll(shardKey);
            if (logger.isInfoEnabled()) {
                logger.info(
                        "Registering shard {} and loaded {} scheduled messages in {} nanoseconds",
                        shardKey,
                        scheduledMessages.size(),
                        System.nanoTime() - startTime);
            }
            ConcurrentHashMap<ScheduledMessageKey, ScheduledFuture<?>> scheduledFuturesForShard =
                    new ConcurrentHashMap<>();
            for (ScheduledMessage scheduledMessage : scheduledMessages) {
                scheduledFuturesForShard.put(
                        scheduledMessage.getKey(),
                        scheduledExecutorService.schedule(
                                new ScheduledMessageRunnable(
                                        key,
                                        scheduledMessage),
                                scheduledMessage.getDelay(TimeUnit.MILLISECONDS),
                                TimeUnit.MILLISECONDS));
            }
            return scheduledFuturesForShard;
        });
    }

    @Override
    public void unregisterShard(ShardKey shardKey) {
        ConcurrentHashMap<ScheduledMessageKey, ScheduledFuture<?>> scheduledFuturesForShard =
                scheduledFutures.remove(shardKey);
        if (scheduledFuturesForShard != null) {
            scheduledFuturesForShard.forEach((k, scheduledFuture) -> scheduledFuture.cancel(false));
        }
    }

    @Override
    public ScheduledMessageRef scheduleOnce(
        Object message,
        ActorRef receiver,
        long delay,
        TimeUnit timeUnit)
    {
        // this method only works when sender is a local persistent actor (so no temp or service actor)
        ActorRef sender = ActorContextHolder.getSelf();
        if(sender instanceof ActorContainerRef) {
            ActorContainer actorContainer = ((ActorContainerRef)sender).getActorContainer();
            if(actorContainer instanceof ActorShard) {
                ActorShard actorShard = (ActorShard) actorContainer;
                if(actorShard.getOwningNode().isLocal()) {
                    // we're in business
                    try {
                        long fireTime = System.currentTimeMillis() + timeUnit.toMillis(delay);
                        MessageSerializer serializer = actorSystem.getSerializer(message.getClass());
                        ByteBuffer serializedMessage = serializer.serialize(message);
                        ScheduledMessage scheduledMessage = new ScheduledMessageImpl(
                            fireTime,
                            sender,
                            receiver,
                            message.getClass(),
                            serializedMessage,
                            message
                        );
                        scheduledMessageRepository.create(actorShard.getKey(), scheduledMessage);
                        schedule(actorShard.getKey(),scheduledMessage);
                        return new ScheduledMessageShardRef(actorSystem.getParent().getClusterName(),actorShard,new ScheduledMessageKey(scheduledMessage.getId(),fireTime));
                    } catch(Exception e) {
                        throw new RejectedExecutionException(e);
                    }
                }
            }
        }
        throw new IllegalStateException(
            "Cannot determine an appropriate ActorRef(self). Only use this method while inside an "
                + "ElasticActor Lifecycle or on(Message) method on a Persistent Actor!");
    }

    @Override
    public ScheduledMessageRef create(String refSpec) {
        final InternalActorSystems cluster = actorSystem.getParent();
        return ScheduledMessageRefTools.parse(refSpec,cluster);
    }

    @Override
    public void cancel(ShardKey shardKey,ScheduledMessageKey messageKey) {
        // sanity check if this is actually a local shard that we manage
        // bit of a hack to send in a broken ScheduledMessage (only the key set)
        unschedule(shardKey,messageKey);
        scheduledMessageRepository.delete(shardKey,messageKey);
    }

    private final class ScheduledMessageRunnable implements Runnable {

        private final ShardKey shardKey;
        private final ScheduledMessage message;

        public ScheduledMessageRunnable(ShardKey shardKey, ScheduledMessage message) {
            this.shardKey = shardKey;
            this.message = message;
        }

        @Override
        public void run() {
            try (MessagingScope ignored = getManager().enter(
                    message.getTraceContext(),
                    forScheduling(message.getCreationContext()))) {
                runInContext();
            }
        }

        public void runInContext() {
            boolean executionInterruptedByResharding = false;
            try {
                final MessageDeserializer messageDeserializer = actorSystem.getDeserializer(message.getMessageClass());
                if(messageDeserializer != null) {
                    Object deserializedMessage = messageDeserializer.deserialize(message.getMessageBytes());
                    // send the message
                    final ActorRef receiverRef = message.getReceiver();
                    receiverRef.tell(deserializedMessage,message.getSender());
                } else {
                    logger.error("Could not find Deserializer for ScheduledMessage of type [{}]",message.getMessageClass().getName());
                }
            } catch(MessageDeliveryException e) {
                // see if it's a recoverable exception
                if(e.isRecoverable()) {
                    // we need to reschedule the message otherwise it will get lost
                    // because we generate a key it will be impossible to cancel, however technically it fired already
                    // so it should be no problem
                    long fireTime = System.currentTimeMillis() + 1000L;
                    ScheduledMessage rescheduledMessage = message.copyForRescheduling(fireTime);
                    try {
                        schedule(shardKey, rescheduledMessage);
                        scheduledMessageRepository.create(shardKey, rescheduledMessage);
                        logger.warn("Got a recoverable MessageDeliveryException, rescheduling ScheduledMessage to fire in 1000 msecs");
                    } catch (RejectedExecutionException re) {
                        executionInterruptedByResharding = true;
                        logger.error("Got a recoverable MessageDeliveryException, but the shard [{}] is not registered anymore. "
                                + "Skipping rescheduling and letting the new owner of the shard try to send the message", shardKey);
                    }
                } else {
                    logger.error("Got an unrecoverable MessageDeliveryException",e);
                }
            } catch(IOException e) {
                // try to figure out what is wrong with the bytes
                String jsonMessage = ByteBufferUtils.decodeUtf8String(message.getMessageBytes());
                logger.error("IOException while deserializing ScheduledMessage contents [{}] of message class [{}]",jsonMessage,message.getMessageClass().getName(),e);
            } catch(Exception e) {
                logger.error("Caught unexpected Exception while exexuting ScheduledMessage",e);
            } finally {
                /*
                 * Always remove from the map of scheduled futures.
                 * Using computeIfPresent as a cheap locking mechanism. Such synchronization is
                 * necessary because this task can execute while the map is still being populated.
                 */
                scheduledFutures.computeIfPresent(shardKey, (key, scheduledFuturesForShard) -> {
                    scheduledFuturesForShard.remove(message.getKey());
                    return scheduledFuturesForShard;
                });
                // Delete the message only if the execution was not interrupted by resharding
                if (!executionInterruptedByResharding) {
                    scheduledMessageRepository.delete(shardKey, message.getKey());
                }
            }
        }
    }
}
