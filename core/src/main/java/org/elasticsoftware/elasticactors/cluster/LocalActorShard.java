/*
 * Copyright 2013 - 2017 The Original Authors
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

package org.elasticsoftware.elasticactors.cluster;

import com.google.common.cache.Cache;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.UncheckedExecutionException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsoftware.elasticactors.*;
import org.elasticsoftware.elasticactors.cache.EvictionListener;
import org.elasticsoftware.elasticactors.cache.ShardActorCacheManager;
import org.elasticsoftware.elasticactors.cluster.scheduler.ScheduledMessageKey;
import org.elasticsoftware.elasticactors.cluster.tasks.*;
import org.elasticsoftware.elasticactors.messaging.*;
import org.elasticsoftware.elasticactors.messaging.internal.ActorNodeMessage;
import org.elasticsoftware.elasticactors.messaging.internal.CancelScheduledMessageMessage;
import org.elasticsoftware.elasticactors.messaging.internal.CreateActorMessage;
import org.elasticsoftware.elasticactors.messaging.internal.DestroyActorMessage;
import org.elasticsoftware.elasticactors.serialization.Message;
import org.elasticsoftware.elasticactors.serialization.MessageDeliveryMode;
import org.elasticsoftware.elasticactors.serialization.MessageSerializer;
import org.elasticsoftware.elasticactors.serialization.SerializationContext;
import org.elasticsoftware.elasticactors.state.PersistentActor;
import org.elasticsoftware.elasticactors.state.PersistentActorRepository;
import org.elasticsoftware.elasticactors.util.ManifestTools;
import org.elasticsoftware.elasticactors.util.concurrent.ThreadBoundExecutor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Configurable;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.dao.EmptyResultDataAccessException;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;

import static org.elasticsoftware.elasticactors.serialization.MessageDeliveryMode.SYSTEM_DEFAULT;
import static org.elasticsoftware.elasticactors.util.SerializationTools.deserializeMessage;

/**
 * @author Joost van de Wijgerd
 */
@Configurable
public final class LocalActorShard extends AbstractActorContainer implements ActorShard, EvictionListener<PersistentActor<ShardKey>> {
    private static final Logger logger = LogManager.getLogger(LocalActorShard.class);
    // this instance acts as a tombstone for stopped actors
    private static final PersistentActor<ShardKey> TOMBSTONE = new PersistentActor<>(null,null,null,null,null,null);
    private final InternalActorSystem actorSystem;
    private final ShardKey shardKey;
    private ThreadBoundExecutor actorExecutor;
    private Cache<ActorRef,PersistentActor<ShardKey>> actorCache;
    private PersistentActorRepository persistentActorRepository;
    private final ShardActorCacheManager actorCacheManager;
    // the cacheloader instance that is reused to avoid garbage being created on each call
    private final CacheLoader cacheLoader = new CacheLoader();

    public LocalActorShard(PhysicalNode node,
                           InternalActorSystem actorSystem,
                           int shard,
                           ActorRef myRef,
                           MessageQueueFactory messageQueueFactory,
                           ShardActorCacheManager actorCacheManager) {
        super(messageQueueFactory, myRef, node);
        this.actorSystem = actorSystem;
        this.actorCacheManager = actorCacheManager;
        this.shardKey = new ShardKey(actorSystem.getName(), shard);
    }

    @Override
    public void init() throws Exception {
        // create cache
        this.actorCache = actorCacheManager.create(shardKey,this);
        // initialize queue
        super.init();
    }

    @Override
    public void destroy() {
        actorCacheManager.destroy(actorCache);
        super.destroy();
    }

    @Override
    public PhysicalNode getOwningNode() {
        return getPhysicalNode();
    }

    @Override
    public ShardKey getKey() {
        return shardKey;
    }

    @Override
    public void onEvicted(PersistentActor<ShardKey> value) {
        // see if it is not a tombstone that gets evicted
        if(!(TOMBSTONE == value)) {
            ElasticActor actorInstance = actorSystem.getActorInstance(value.getSelf(), value.getActorClass());
            actorExecutor.execute(new PassivateActorTask(persistentActorRepository, value, actorSystem, actorInstance, value.getSelf()));
        }
    }

    public void sendMessage(ActorRef from, List<? extends ActorRef> to, Object message) throws Exception {
        InternalMessage internalMessage = createInternalMessage(from, to, message);
        if (internalMessage != null) messageQueue.offer(internalMessage);
    }

    private InternalMessage createInternalMessage(ActorRef from, List<? extends ActorRef> to, Object message) throws IOException {
        MessageSerializer<Object> messageSerializer = (MessageSerializer<Object>) actorSystem.getSerializer(message.getClass());
        if(messageSerializer == null) {
            logger.error(String.format("No message serializer found for class: %s. NOT sending message",
                    message.getClass().getSimpleName()));
            return null;
        }
        // get the durable flag
        Message messageAnnotation = message.getClass().getAnnotation(Message.class);
        final boolean durable = (messageAnnotation != null) && messageAnnotation.durable();
        final int timeout = (messageAnnotation != null) ? messageAnnotation.timeout() : Message.NO_TIMEOUT;
        MessageDeliveryMode deliveryMode = (messageAnnotation == null || messageAnnotation.deliveryMode() == SYSTEM_DEFAULT) ? actorSystem.getConfiguration().getMessageDeliveryMode() : messageAnnotation.deliveryMode();
        return new InternalMessageImpl(from, ImmutableList.copyOf(to), SerializationContext.serialize(messageSerializer, message),message.getClass().getName(),durable, timeout, deliveryMode);
    }

    @Override
    public void undeliverableMessage(InternalMessage message, ActorRef receiverRef) throws Exception {
        // input is the message that cannot be delivered
        InternalMessage undeliverableMessage;
        if (message instanceof TransientInternalMessage) {
            undeliverableMessage = new TransientInternalMessage(receiverRef, message.getSender(), message.getPayload(null), true, message.getDeliveryMode());
        } else {
            undeliverableMessage = new InternalMessageImpl( receiverRef,
                                                            message.getSender(),
                                                            message.getPayload(),
                                                            message.getPayloadClass(),
                                                            message.isDurable(),
                                                            true,
                                                            message.getTimeout(),
                                                            message.getDeliveryMode());
        }
        messageQueue.offer(undeliverableMessage);
    }

    @Override
    public void handleMessage(final InternalMessage im,
                              final MessageHandlerEventListener mhel) {
        // we only need to copy if there are more than one receivers
        boolean needsCopy = false;
        MessageHandlerEventListener messageHandlerEventListener = mhel;
        if(im.getReceivers().size() > 1) {
            needsCopy = true;
            messageHandlerEventListener = new MultiMessageHandlerEventListener(mhel, im.getReceivers().size());
        }
        for (ActorRef receiverRef : im.getReceivers()) {
            InternalMessage internalMessage = (needsCopy) ? im.copyOf() : im;
            if (receiverRef.getActorId() != null) {
                // make sure we load the right value from the cache
                cacheLoader.initialize(receiverRef);
                try {
                    // load persistent actor from cache or persistent store
                    PersistentActor<ShardKey> actor = actorCache.get(receiverRef, cacheLoader);
                    // see if we don't have a recently destroyed actor
                    if (TOMBSTONE == actor) {
                        try {
                            handleUndeliverable(internalMessage, receiverRef, messageHandlerEventListener);
                        } catch (Exception ex) {
                            logger.error("Exception while sending message undeliverable", ex);
                        }
                    } else {
                        // find actor class behind receiver ActorRef
                        ElasticActor actorInstance = actorSystem.getActorInstance(receiverRef, actor.getActorClass());
                        // execute on it's own thread
                        if (internalMessage.isUndeliverable()) {
                            actorExecutor.execute(new HandleUndeliverableMessageTask(actorSystem,
                                    actorInstance,
                                    receiverRef,
                                    internalMessage,
                                    actor,
                                    persistentActorRepository,
                                    messageHandlerEventListener));
                        } else {
                            actorExecutor.execute(new HandleMessageTask(actorSystem,
                                    actorInstance,
                                    receiverRef,
                                    internalMessage,
                                    actor,
                                    persistentActorRepository,
                                    messageHandlerEventListener));
                        }
                    }
                } catch (UncheckedExecutionException e) {
                    if (e.getCause() instanceof EmptyResultDataAccessException) {
                        try {
                            handleUndeliverable(internalMessage, receiverRef, messageHandlerEventListener);
                        } catch (Exception ex) {
                            logger.error("Exception while sending message undeliverable", ex);
                        }
                    } else {
                        messageHandlerEventListener.onError(internalMessage, e.getCause());
                        logger.error(String.format("Exception while handling InternalMessage for Actor [%s]; senderRef [%s], messageType [%s]", receiverRef.getActorId(), internalMessage.getSender(), internalMessage.getPayloadClass()), e.getCause());
                    }
                } catch (Exception e) {
                    //@todo: let the sender know his message could not be delivered
                    // we ack the message anyway
                    messageHandlerEventListener.onError(internalMessage, e);
                    logger.error(String.format("Exception while handling InternalMessage for Actor [%s]; senderRef [%s], messageType [%s]", receiverRef.getActorId(), internalMessage.getSender(), internalMessage.getPayloadClass()), e.getCause());
                } finally {
                    // ensure the cacheLoader is reset
                    this.cacheLoader.reset();
                }
            } else {
                // the internalMessage is intended for the shard, this means it's about creating or destroying an actor
                // or cancelling a scheduled message which will piggyback on the ActorShard messaging layer
                // or forwarding a reply for a Temp- or ServiceActor from a remote system
                try {
                    Object message = deserializeMessage(actorSystem, internalMessage);
                    // check if the actor exists
                    if (message instanceof CreateActorMessage) {
                        CreateActorMessage createActorMessage = (CreateActorMessage) message;
                        if (!actorExists(actorSystem.actorFor(createActorMessage.getActorId()))) {
                            createActor(createActorMessage, internalMessage, messageHandlerEventListener);
                        } else {
                            // we need to activate the actor since we need to run the postActivate logic
                            activateActor(actorSystem.actorFor(createActorMessage.getActorId()));
                            // ack message anyway
                            messageHandlerEventListener.onDone(internalMessage);
                        }
                    } else if (message instanceof DestroyActorMessage) {
                        DestroyActorMessage destroyActorMessage = (DestroyActorMessage) message;
                        if (actorExists(destroyActorMessage.getActorRef())) {
                            destroyActor(destroyActorMessage, internalMessage, messageHandlerEventListener);
                        } else {
                            // ack message anyway
                            messageHandlerEventListener.onDone(internalMessage);
                        }
                    } else if (message instanceof CancelScheduledMessageMessage) {
                        CancelScheduledMessageMessage cancelMessage = (CancelScheduledMessageMessage) message;
                        actorSystem.getInternalScheduler().cancel(this.shardKey, new ScheduledMessageKey(cancelMessage.getMessageId(), cancelMessage.getFireTime()));
                        // ack the message
                        messageHandlerEventListener.onDone(internalMessage);
                    } else if(message instanceof ActorNodeMessage) {
                        if(!internalMessage.isUndeliverable()) {
                            ActorNodeMessage actorNodeMessage = (ActorNodeMessage) message;
                            ActorNode actorNode = actorSystem.getNode(actorNodeMessage.getNodeId());
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
                        // ack
                        messageHandlerEventListener.onDone(internalMessage);
                    }
                } catch (Exception e) {
                    // @todo: determine if this is a recoverable error case or just a programming error
                    messageHandlerEventListener.onError(internalMessage, e);
                    logger.error(String.format("Exception while handling InternalMessage for Shard [%s]; senderRef [%s], messageType [%s]", shardKey.toString(), internalMessage.getSender().toString(), internalMessage.getPayloadClass()), e);
                }

            }
        }
    }

    private boolean actorExists(ActorRef actorRef) {
        PersistentActor<ShardKey> persistentActor = actorCache.getIfPresent(actorRef);
        if(persistentActor != null) {
            return !(TOMBSTONE == persistentActor);
        } else {
            return persistentActorRepository.contains(shardKey, actorRef.getActorId());
        }
    }

    private void createActor(CreateActorMessage createMessage,InternalMessage internalMessage, MessageHandlerEventListener messageHandlerEventListener) throws Exception {
        ActorRef ref = actorSystem.actorFor(createMessage.getActorId());
        final Class<? extends ElasticActor> actorClass = (Class<? extends ElasticActor>) Class.forName(createMessage.getActorClass());
        final String actorStateVersion = ManifestTools.extractActorStateVersion(actorClass);
        PersistentActor<ShardKey> persistentActor =
                new PersistentActor<>(shardKey, actorSystem, actorStateVersion, ref, actorClass, createMessage.getInitialState());

        // Actor state is now created in the create actor task
        // persistentActorRepository.update(this.shardKey,persistentActor);

        actorCache.put(ref,persistentActor);
        // find actor class behind receiver ActorRef
        ElasticActor actorInstance = actorSystem.getActorInstance(ref,persistentActor.getActorClass());
        // call postCreate
        actorExecutor.execute(new CreateActorTask(persistentActorRepository,
                                                  persistentActor,
                                                  actorSystem,
                                                  actorInstance,
                                                  ref,
                                                  internalMessage,
                                                  messageHandlerEventListener));
    }

    private void activateActor(final ActorRef actorRef) throws Exception {
        // prepare the cache loader
        this.cacheLoader.initialize(actorRef);
        try {
            actorCache.get(actorRef,cacheLoader);
        } finally{
            this.cacheLoader.reset();
        }
    }

    private void destroyActor(DestroyActorMessage destroyMessage,InternalMessage internalMessage, MessageHandlerEventListener messageHandlerEventListener) throws Exception {
        final ActorRef actorRef = destroyMessage.getActorRef();
        this.cacheLoader.initialize(actorRef);
        try {
            // need to load it here to know the ActorClass!
            PersistentActor<ShardKey> persistentActor = actorCache.get(actorRef,cacheLoader);
            // we used to delete actor state here to avoid race condition
            /*
            persistentActorRepository.delete(this.shardKey,actorRef.getActorId());
            actorCache.invalidate(actorRef);
            // seems like we need to call an explicit cleanup for the cache to clear
            actorCache.cleanUp();
            */
            // now we handle it in the destroy task, but mark the actor as destroyed
            actorCache.put(actorRef,TOMBSTONE);
            // find actor class behind receiver ActorRef
            ElasticActor actorInstance = actorSystem.getActorInstance(actorRef,persistentActor.getActorClass());
            // call preDestroy
            actorExecutor.execute(new DestroyActorTask( persistentActorRepository,
                                                        persistentActor,
                                                        actorSystem,
                                                        actorInstance,
                                                        actorRef,
                                                        internalMessage,
                                                        messageHandlerEventListener));
        } finally {
            this.cacheLoader.reset();
        }
    }

    @Autowired
    public void setActorExecutor(@Qualifier("actorExecutor") ThreadBoundExecutor actorExecutor) {
        this.actorExecutor = actorExecutor;
    }

    @Autowired
    public void setPersistentActorRepository(PersistentActorRepository persistentActorRepository) {
        this.persistentActorRepository = persistentActorRepository;
    }

    /**
     * To avoid creation of these callable, we cache it at the Shard instance, setting the values as necesser
     *
     */
    private final class CacheLoader implements Callable<PersistentActor<ShardKey>> {
        // @todo: don't think this needs to be volatile as all access will happen in one thread only
        private ActorRef actorRef = null;

        public CacheLoader() {
        }

        public void initialize(ActorRef actorRef) {
            this.actorRef = actorRef;
        }

        public void reset() {
            this.actorRef = null;
        }

        @Override
        public PersistentActor<ShardKey> call() throws Exception {
            PersistentActor<ShardKey> loadedActor = persistentActorRepository.get(shardKey, actorRef.getActorId());
            if(loadedActor == null) {
                // @todo: using Spring DataAccesException here, might want to change this or use in Repository implementation
                throw new EmptyResultDataAccessException(String.format("Actor [%s] not found in Shard [%s]", actorRef.getActorId(), shardKey.toString()),1);
            } else {
                ElasticActor actorInstance = actorSystem.getActorInstance(actorRef,
                                                                          loadedActor.getActorClass());
                actorExecutor.execute(new ActivateActorTask(persistentActorRepository,
                                                            loadedActor,
                                                            actorSystem,
                                                            actorInstance,
                                                            actorRef));
                return loadedActor;
            }
        }
    }
}

