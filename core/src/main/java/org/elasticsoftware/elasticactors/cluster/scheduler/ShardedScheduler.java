/*
 *   Copyright 2013 - 2019 The Original Authors
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

package org.elasticsoftware.elasticactors.cluster.scheduler;

import org.elasticsoftware.elasticactors.ActorContainer;
import org.elasticsoftware.elasticactors.ActorContainerRef;
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.ActorShard;
import org.elasticsoftware.elasticactors.MessageDeliveryException;
import org.elasticsoftware.elasticactors.ShardKey;
import org.elasticsoftware.elasticactors.cluster.InternalActorSystem;
import org.elasticsoftware.elasticactors.cluster.InternalActorSystems;
import org.elasticsoftware.elasticactors.scheduler.ScheduledMessageRef;
import org.elasticsoftware.elasticactors.serialization.MessageDeserializer;
import org.elasticsoftware.elasticactors.serialization.MessageSerializer;
import org.elasticsoftware.elasticactors.util.concurrent.DaemonThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;

/**
 * @author Joost van de Wijgerd
 */
public final class ShardedScheduler implements SchedulerService,ScheduledMessageRefFactory {
    private static final Logger logger = LoggerFactory.getLogger(ShardedScheduler.class);
    private ScheduledMessageRepository scheduledMessageRepository;
    private InternalActorSystem actorSystem;
    private ScheduledExecutorService scheduledExecutorService;
    private final ConcurrentMap<ShardKey, ConcurrentHashMap<ScheduledMessageKey, ScheduledFuture<?>>> scheduledFutures = new ConcurrentHashMap<>();
    private final int numberOfWorkers;

    @PostConstruct
    public void init() {
        scheduledExecutorService = Executors.newScheduledThreadPool(numberOfWorkers, new DaemonThreadFactory("SCHEDULER"));
    }

    @PreDestroy
    public void destroy() {
        scheduledExecutorService.shutdownNow();
    }

    public ShardedScheduler() {
        this(Runtime.getRuntime().availableProcessors());
    }

    public ShardedScheduler(int numberOfWorkers) {
        this.numberOfWorkers = numberOfWorkers;
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
        ConcurrentHashMap<ScheduledMessageKey, ScheduledFuture<?>> scheduledFuturesForShard =
                scheduledFutures.get(shardKey);
        if (scheduledFuturesForShard != null) {
            ScheduledFuture<?> previous =
                    scheduledFuturesForShard.get(scheduledMessage.getKey());
            if (previous == null || previous.cancel(false)) {
                scheduledFuturesForShard.put(
                        scheduledMessage.getKey(),
                        scheduledExecutorService.schedule(
                                new ScheduledMessageRunnable(
                                        shardKey,
                                        scheduledMessage),
                                scheduledMessage.getDelay(TimeUnit.MILLISECONDS),
                                TimeUnit.MILLISECONDS));
            }
        }
    }

    private void unschedule(ShardKey shardKey, ScheduledMessageKey messageKey) {
        ConcurrentHashMap<ScheduledMessageKey, ScheduledFuture<?>> scheduledFuturesForShard =
                scheduledFutures.get(shardKey);
        if (scheduledFuturesForShard != null) {
            ScheduledFuture<?> scheduledFuture = scheduledFuturesForShard.remove(messageKey);
            if (scheduledFuture != null) {
                scheduledFuture.cancel(false);
            }
        }
    }

    @Override
    public void registerShard(ShardKey shardKey) {
        // obtain the scheduler shard
        scheduledFutures.computeIfAbsent(shardKey, key -> new ConcurrentHashMap<>());
        // fetch block from repository
        // @todo: for now we'll fetch all, this obviously has memory issues
        long startTime = System.nanoTime();
        List<ScheduledMessage> scheduledMessages = scheduledMessageRepository.getAll(shardKey);
        if(logger.isInfoEnabled()) {
            logger.info("Registering shard {} and loaded {} scheduled messages in {} nanoseconds",
                    shardKey,
                    scheduledMessages.size(),
                    System.nanoTime() - startTime);
        }
        for (ScheduledMessage message : scheduledMessages) {
            schedule(shardKey, message);
        }
    }

    @Override
    public void unregisterShard(ShardKey shardKey) {
        ConcurrentHashMap<ScheduledMessageKey, ScheduledFuture<?>> scheduledFuturesForShard =
                scheduledFutures.remove(shardKey);
        if (scheduledFuturesForShard != null) {
            for (ScheduledFuture<?> scheduledFuture : scheduledFuturesForShard.values()) {
                scheduledFuture.cancel(false);
            }
        }
    }

    @Override
    public ScheduledMessageRef scheduleOnce(ActorRef sender, Object message, ActorRef receiver, long delay, TimeUnit timeUnit) {
        // this method only works when sender is a local persistent actor (so no temp or service actor)
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
                        byte[] serializedBytes = new byte[serializedMessage.remaining()];
                        serializedMessage.get(serializedBytes);
                        ScheduledMessage scheduledMessage = new ScheduledMessageImpl(fireTime,sender,receiver,message.getClass(),serializedBytes);
                        scheduledMessageRepository.create(actorShard.getKey(), scheduledMessage);
                        schedule(actorShard.getKey(),scheduledMessage);
                        return new ScheduledMessageShardRef(actorSystem.getParent().getClusterName(),actorShard,new ScheduledMessageKey(scheduledMessage.getId(),fireTime));
                    } catch(Exception e) {
                        throw new RejectedExecutionException(e);
                    }
                }
            }
        }
        // sender param didn't fit the criteria
        throw new IllegalArgumentException(format("sender ref: %s needs to be a non-temp, non-service, locally sharded actor ref", sender));
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
            try {
                final MessageDeserializer messageDeserializer = actorSystem.getDeserializer(message.getMessageClass());
                if(messageDeserializer != null) {
                    Object deserializedMessage = messageDeserializer.deserialize(ByteBuffer.wrap(message.getMessageBytes()));
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
                    ScheduledMessage rescheduledMessage = new ScheduledMessageImpl(fireTime,message.getSender(),message.getReceiver(),message.getMessageClass(),message.getMessageBytes());
                    scheduledMessageRepository.create(shardKey, rescheduledMessage);
                    schedule(shardKey,rescheduledMessage);
                    logger.warn("Got a recoverable MessageDeliveryException, rescheduling ScheduledMessage to fire in 1000 msecs");
                } else {
                    logger.error("Got an unrecoverable MessageDeliveryException",e);
                }
            } catch(IOException e) {
                // try to figure out what is wrong with the bytes
                String jsonMessage = new String(message.getMessageBytes(), StandardCharsets.UTF_8);
                logger.error("IOException while deserializing ScheduledMessage contents [{}] of message class [{}]",jsonMessage,message.getMessageClass().getName(),e);
            } catch(Exception e) {
                logger.error("Caught unexpected Exception while exexuting ScheduledMessage",e);
            } finally {
                // always remove from the backing store
                scheduledMessageRepository.delete(shardKey, message.getKey());
                // always remove from the map of scheduled futures
                ConcurrentHashMap<ScheduledMessageKey, ScheduledFuture<?>> mapForShard =
                        scheduledFutures.get(shardKey);
                if (mapForShard != null) {
                    mapForShard.remove(message.getKey());
                }
            }
        }
    }
}
