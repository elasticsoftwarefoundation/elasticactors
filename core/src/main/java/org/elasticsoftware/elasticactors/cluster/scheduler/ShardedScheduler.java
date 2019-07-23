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
import org.elasticsoftware.elasticactors.util.concurrent.ShardedScheduledWorkManager;
import org.elasticsoftware.elasticactors.util.concurrent.WorkExecutor;
import org.elasticsoftware.elasticactors.util.concurrent.WorkExecutorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;

/**
 * @author Joost van de Wijgerd
 */
public final class ShardedScheduler implements SchedulerService,WorkExecutorFactory,ScheduledMessageRefFactory {
    private static final Logger logger = LoggerFactory.getLogger(ShardedScheduler.class);
    private ShardedScheduledWorkManager<ShardKey,ScheduledMessage> workManager;
    private ScheduledMessageRepository scheduledMessageRepository;
    private InternalActorSystem actorSystem;


    @PostConstruct
    public void init() {
        ExecutorService executorService = Executors.newCachedThreadPool(new DaemonThreadFactory("SCHEDULER"));
        workManager = new ShardedScheduledWorkManager<>(executorService,this,Runtime.getRuntime().availableProcessors());
        workManager.init();
    }

    @PreDestroy
    public void destroy() {
        workManager.destroy();
    }

    @Inject
    public void setScheduledMessageRepository(ScheduledMessageRepository scheduledMessageRepository) {
        this.scheduledMessageRepository = scheduledMessageRepository;
    }

    @Inject
    public void setActorSystem(InternalActorSystem actorSystem) {
        this.actorSystem = actorSystem;
    }

    @Override
    public void registerShard(ShardKey shardKey) {
        // obtain the scheduler shard
        workManager.registerShard(shardKey);
        // fetch block from repository
        // @todo: for now we'll fetch all, this obviously has memory issues
        List<ScheduledMessage> scheduledMessages = scheduledMessageRepository.getAll(shardKey);
        workManager.schedule(shardKey,scheduledMessages.toArray(new ScheduledMessage[0]));
    }

    @Override
    public void unregisterShard(ShardKey shardKey) {
        workManager.unregisterShard(shardKey);
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
                        long fireTime = System.currentTimeMillis() + TimeUnit.MILLISECONDS.convert(delay,timeUnit);
                        MessageSerializer serializer = actorSystem.getSerializer(message.getClass());
                        ByteBuffer serializedMessage = serializer.serialize(message);
                        byte[] serializedBytes = new byte[serializedMessage.remaining()];
                        serializedMessage.get(serializedBytes);
                        ScheduledMessage scheduledMessage = new ScheduledMessageImpl(fireTime,sender,receiver,message.getClass(),serializedBytes);
                        scheduledMessageRepository.create(actorShard.getKey(), scheduledMessage);
                        workManager.schedule(actorShard.getKey(),scheduledMessage);
                        return new ScheduledMessageShardRef(actorSystem.getParent().getClusterName(),actorShard,new ScheduledMessageKey(scheduledMessage.getId(),fireTime));
                    } catch(Exception e) {
                        throw new RejectedExecutionException(e);
                    }
                }
            }
        }
        // sender param didn't fit the criteria
        throw new IllegalArgumentException(format("sender ref: %s needs to be a non-temp, non-service, locally sharded actor ref",(sender == null) ? "null" : sender.toString()));
    }

    @Override
    public WorkExecutor create() {
        return new ScheduledMessageExecutor();
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
        workManager.unschedule(shardKey,new ScheduledMessageImpl(messageKey.getId(),messageKey.getFireTime()));
        scheduledMessageRepository.delete(shardKey,messageKey);
    }

    private final class ScheduledMessageExecutor implements WorkExecutor<ShardKey,ScheduledMessage> {

        @Override
        public void execute(final ShardKey shardKey,final ScheduledMessage message) {
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
                    workManager.schedule(shardKey,rescheduledMessage);
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
            }
        }
    }
}
