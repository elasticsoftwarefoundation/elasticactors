/*
 *   Copyright 2013 - 2022 The Original Authors
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

package org.elasticsoftware.elasticactors.cluster;

import com.google.common.cache.Cache;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.UncheckedExecutionException;
import org.elasticsoftware.elasticactors.ActorNode;
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.ActorShard;
import org.elasticsoftware.elasticactors.ElasticActor;
import org.elasticsoftware.elasticactors.PhysicalNode;
import org.elasticsoftware.elasticactors.ShardKey;
import org.elasticsoftware.elasticactors.cache.EvictionListener;
import org.elasticsoftware.elasticactors.cache.ShardActorCacheManager;
import org.elasticsoftware.elasticactors.cluster.logging.LoggingSettings;
import org.elasticsoftware.elasticactors.cluster.metrics.MetricsSettings;
import org.elasticsoftware.elasticactors.cluster.scheduler.ScheduledMessageKey;
import org.elasticsoftware.elasticactors.cluster.tasks.ActivateActorTask;
import org.elasticsoftware.elasticactors.cluster.tasks.CreateActorTask;
import org.elasticsoftware.elasticactors.cluster.tasks.DestroyActorTask;
import org.elasticsoftware.elasticactors.cluster.tasks.PassivateActorTask;
import org.elasticsoftware.elasticactors.cluster.tasks.PersistActorTask;
import org.elasticsoftware.elasticactors.messaging.InternalMessage;
import org.elasticsoftware.elasticactors.messaging.InternalMessageFactory;
import org.elasticsoftware.elasticactors.messaging.MessageHandlerEventListener;
import org.elasticsoftware.elasticactors.messaging.MessageQueueFactory;
import org.elasticsoftware.elasticactors.messaging.MultiMessageHandlerEventListener;
import org.elasticsoftware.elasticactors.messaging.internal.ActorNodeMessage;
import org.elasticsoftware.elasticactors.messaging.internal.CancelScheduledMessageMessage;
import org.elasticsoftware.elasticactors.messaging.internal.CreateActorMessage;
import org.elasticsoftware.elasticactors.messaging.internal.DestroyActorMessage;
import org.elasticsoftware.elasticactors.messaging.internal.PersistActorMessage;
import org.elasticsoftware.elasticactors.state.ActorStateUpdateProcessor;
import org.elasticsoftware.elasticactors.state.PersistentActor;
import org.elasticsoftware.elasticactors.state.PersistentActorRepository;
import org.elasticsoftware.elasticactors.util.ManifestTools;
import org.elasticsoftware.elasticactors.util.concurrent.ThreadBoundExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.EmptyResultDataAccessException;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import static org.elasticsoftware.elasticactors.cluster.tasks.ProtocolFactoryFactory.getProtocolFactory;
import static org.elasticsoftware.elasticactors.util.ClassLoadingHelper.getClassHelper;
import static org.elasticsoftware.elasticactors.util.SerializationTools.deserializeMessage;

/**
 * @author Joost van de Wijgerd
 */
public final class LocalActorShard extends AbstractActorContainer implements ActorShard, EvictionListener<PersistentActor<ShardKey>> {

    private static final Logger staticLogger = LoggerFactory.getLogger(LocalActorShard.class);

    // this instance acts as a tombstone for stopped actors
    private static final PersistentActor<ShardKey> TOMBSTONE = new PersistentActor<>(null,null,null,null,null,null);
    private final InternalActorSystem actorSystem;
    private final ShardKey shardKey;
    private final ThreadBoundExecutor actorExecutor;
    private Cache<ActorRef,PersistentActor<ShardKey>> actorCache;
    private final PersistentActorRepository persistentActorRepository;
    private final ActorStateUpdateProcessor actorStateUpdateProcessor;
    private final ShardActorCacheManager actorCacheManager;
    private final MetricsSettings metricsSettings;
    private final LoggingSettings loggingSettings;

    public LocalActorShard(
        PhysicalNode node,
        InternalActorSystem actorSystem,
        int shard,
        ActorRef myRef,
        MessageQueueFactory messageQueueFactory,
        ThreadBoundExecutor actorExecutor,
        PersistentActorRepository persistentActorRepository,
        ActorStateUpdateProcessor actorStateUpdateProcessor,
        ShardActorCacheManager actorCacheManager,
        MetricsSettings metricsSettings,
        LoggingSettings loggingSettings)
    {
        super(
            messageQueueFactory,
            myRef,
            node,
            actorSystem.getQueuesPerShard(),
            actorSystem.getMultiQueueHashSeed()
        );
        this.actorSystem = actorSystem;
        this.actorExecutor = actorExecutor;
        this.persistentActorRepository = persistentActorRepository;
        this.actorStateUpdateProcessor = actorStateUpdateProcessor;
        this.actorCacheManager = actorCacheManager;
        this.metricsSettings = metricsSettings;
        this.loggingSettings = loggingSettings;
        this.shardKey = new ShardKey(actorSystem.getName(), shard);
    }

    @Override
    protected Logger initLogger() {
        return staticLogger;
    }

    @Override
    public synchronized void init() throws Exception {
        logger.info("Initializing Local Actor Shard [{}]", shardKey);
        // create cache
        this.actorCache = actorCacheManager.create(shardKey,this);
        // initialize queue
        super.init();
    }

    @Override
    public void destroy() {
        logger.info("Destroying Local Actor Shard [{}]", shardKey);
        super.destroy();
        actorCacheManager.destroy(actorCache);
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
            actorExecutor.execute(new PassivateActorTask(actorStateUpdateProcessor, persistentActorRepository, value,
                    actorSystem, actorInstance, value.getSelf()));
        } else {
            logger.debug("Evicted [{}] of type [{}]", value.getSelf(), value.getActorClass());
        }
    }

    @Override
    public void sendMessage(ActorRef from, List<? extends ActorRef> to, Object message) throws Exception {
        InternalMessage internalMessage;
        if (message instanceof CreateActorMessage) {
            // Since we cannot guarantee the initial state is an immutable object, always serialize
            internalMessage =
                InternalMessageFactory.createWithSerializedPayload(from, to, actorSystem, message);
        } else {
            internalMessage = InternalMessageFactory.create(from, to, actorSystem, message);
        }
        offerInternalMessage(internalMessage);
    }

    @Override
    public void undeliverableMessage(InternalMessage message, ActorRef receiverRef) throws Exception {
        // input is the message that cannot be delivered
        InternalMessage undeliverableMessage =
            InternalMessageFactory.copyForUndeliverable(message, receiverRef);
        offerInternalMessage(undeliverableMessage);
    }

    @Override
    public void handleMessage(
        final InternalMessage internalMessage,
        final MessageHandlerEventListener mhel)
    {
        MessageHandlerEventListener messageHandlerEventListener = mhel;
        if(internalMessage.getReceivers().size() > 1) {
            messageHandlerEventListener =
                new MultiMessageHandlerEventListener(mhel, internalMessage.getReceivers().size());
        }
        for (ActorRef receiverRef : internalMessage.getReceivers()) {
            if (receiverRef.getActorId() != null) {
                try {
                    // load persistent actor from cache or persistent store
                    PersistentActor<ShardKey> actor = loadActorFromCache(receiverRef);
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
                        // execute on its own thread
                        if (internalMessage.isUndeliverable()) {
                            actorExecutor.execute(getProtocolFactory(internalMessage)
                                .createHandleUndeliverableMessageTask(
                                    actorSystem,
                                    actorInstance,
                                    receiverRef,
                                    internalMessage,
                                    actor,
                                    persistentActorRepository,
                                    messageHandlerEventListener,
                                    metricsSettings,
                                    loggingSettings
                                ));
                        } else {
                            actorExecutor.execute(getProtocolFactory(internalMessage)
                                .createHandleMessageTask(
                                    actorSystem,
                                    actorInstance,
                                    receiverRef,
                                    internalMessage,
                                    actor,
                                    persistentActorRepository,
                                    actorStateUpdateProcessor,
                                    messageHandlerEventListener,
                                    metricsSettings,
                                    loggingSettings
                                ));
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
                        logger.error("Exception while handling InternalMessage for Actor [{}]; senderRef [{}], messageType [{}]", receiverRef.getActorId(), internalMessage.getSender(), internalMessage.getPayloadClass(), e.getCause());
                    }
                } catch (Exception e) {
                    //@todo: let the sender know his message could not be delivered
                    // we ack the message anyway
                    messageHandlerEventListener.onError(internalMessage, e);
                    logger.error(
                        "Exception while handling InternalMessage for Actor [{}]; senderRef [{}],"
                            + " messageType [{}]",
                        receiverRef.getActorId(),
                        internalMessage.getSender(),
                        internalMessage.getPayloadClass(),
                        e
                    );
                }
            } else {
                // the internalMessage is intended for the shard, this means it's about creating or destroying an actor
                // or cancelling a scheduled message which will piggyback on the ActorShard messaging layer
                // or forwarding a reply for a Temp- or ServiceActor from a remote system
                // or a request to persist the state of an actor
                try {
                    Object message = deserializeMessage(actorSystem, internalMessage);
                    // check if the actor exists
                    if (message instanceof CreateActorMessage) {
                        CreateActorMessage createActorMessage = (CreateActorMessage) message;
                        ActorRef actorRef = actorSystem.actorFor(createActorMessage.getActorId());
                        if (!actorExists(actorRef)) {
                            createActor(
                                actorRef,
                                createActorMessage,
                                internalMessage,
                                messageHandlerEventListener
                            );
                        } else {
                            // we need to activate the actor since we need to run the postActivate logic
                            activateActor(actorRef);
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
                                    InternalMessage originalMessage = InternalMessageFactory.create(
                                        actorNodeMessage.getReceiverRef(),
                                        internalMessage.getSender() != null
                                            ? ImmutableList.of(internalMessage.getSender())
                                            : ImmutableList.of(),
                                        actorSystem,
                                        actorNodeMessage.getMessage()
                                    );
                                    actorNode.undeliverableMessage(originalMessage, internalMessage.getSender());
                                }
                            } else {
                                // @todo: we currently don't handle message undeliverable for ActorNodeMessages
                                logger.error("ActorNode with id [{}] is not reachable, discarding message of type [{}] from [{}] for [{}]",
                                        actorNodeMessage.getNodeId(), actorNodeMessage.getMessage().getClass().getName(), internalMessage.getSender(),
                                        actorNodeMessage.getReceiverRef());
                            }
                        } else {
                            // @todo: we currently don't handle message undeliverable for ActorNodeMessages
                            logger.error("undeliverable ActorNodeMessages are currently not supported");
                        }
                        // ack
                        messageHandlerEventListener.onDone(internalMessage);
                    } else if(message instanceof PersistActorMessage) {
                        PersistActorMessage persistMessage = (PersistActorMessage) message;
                        persistActor(persistMessage, internalMessage, messageHandlerEventListener);
                    } else {
                        // unknown internal message, just ack it (should not happen)
                        messageHandlerEventListener.onDone(internalMessage);
                    }
                } catch (Exception e) {
                    // @todo: determine if this is a recoverable error case or just a programming error
                    messageHandlerEventListener.onError(internalMessage, e);
                    logger.error("Exception while handling InternalMessage for Shard [{}]; senderRef [{}], messageType [{}]", shardKey, internalMessage.getSender(), internalMessage.getPayloadClass(), e);
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

    private void createActor(
        ActorRef ref,
        CreateActorMessage createMessage,
        InternalMessage internalMessage,
        MessageHandlerEventListener messageHandlerEventListener) throws Exception
    {
        final Class<? extends ElasticActor> actorClass =
            (Class<? extends ElasticActor>) getClassHelper().forName(createMessage.getActorClass());
        final String actorStateVersion = ManifestTools.extractActorStateVersion(actorClass);
        PersistentActor<ShardKey> persistentActor =
                new PersistentActor<>(shardKey, actorSystem, actorStateVersion, ref, actorClass, createMessage.getInitialState());

        // Actor state is now created in the create actor task
        // persistentActorRepository.update(this.shardKey,persistentActor);

        actorCache.put(ref,persistentActor);
        // find actor class behind receiver ActorRef
        ElasticActor actorInstance = actorSystem.getActorInstance(ref,persistentActor.getActorClass());
        // call postCreate
        actorExecutor.execute(new CreateActorTask(actorStateUpdateProcessor,
                                                  persistentActorRepository,
                                                  persistentActor,
                                                  actorSystem,
                                                  actorInstance,
                                                  ref,
                                                  internalMessage,
                                                  messageHandlerEventListener));
    }

    private void activateActor(final ActorRef actorRef) throws ExecutionException {
        loadActorFromCache(actorRef);
    }

    private void destroyActor(DestroyActorMessage destroyMessage,InternalMessage internalMessage, MessageHandlerEventListener messageHandlerEventListener) throws Exception {
        final ActorRef actorRef = destroyMessage.getActorRef();
        // need to load it here to know the ActorClass!
        PersistentActor<ShardKey> persistentActor = loadActorFromCache(actorRef);
        // we used to delete actor state here to avoid race condition
            /*
            persistentActorRepository.delete(this.shardKey,actorRef.getActorId());
            actorCache.invalidate(actorRef);
            // seems like we need to call an explicit cleanup for the cache to clear
            actorCache.cleanUp();
            */
        // now we handle it in the destroy task, but mark the actor as destroyed
        actorCache.put(actorRef, TOMBSTONE);
        // find actor class behind receiver ActorRef
        ElasticActor actorInstance =
            actorSystem.getActorInstance(actorRef, persistentActor.getActorClass());
        // call preDestroy
        actorExecutor.execute(new DestroyActorTask(
            actorStateUpdateProcessor,
            persistentActorRepository,
            persistentActor,
            actorSystem,
            actorInstance,
            actorRef,
            internalMessage,
            messageHandlerEventListener
        ));
    }

    private void persistActor(
        PersistActorMessage persistMessage,
        InternalMessage internalMessage,
        MessageHandlerEventListener messageHandlerEventListener) throws ExecutionException
    {
        final ActorRef actorRef = persistMessage.getActorRef();
        // need to load it here to know the ActorClass!
        PersistentActor<ShardKey> persistentActor = loadActorFromCache(actorRef);
        // find actor class behind receiver ActorRef
        ElasticActor actorInstance =
            actorSystem.getActorInstance(actorRef, persistentActor.getActorClass());
        // call preDestroy
        actorExecutor.execute(new PersistActorTask(
            actorStateUpdateProcessor,
            persistentActorRepository,
            persistentActor,
            actorSystem,
            actorInstance,
            actorRef,
            internalMessage,
            messageHandlerEventListener
        ));
    }

    // Pooling objects, so we don't create a lot of garbage
    // Making it ThreadLocal to ensure no threads can get the same object in the pool
    private static final ThreadLocal<CacheLoader> cacheLoaderPool = ThreadLocal.withInitial(CacheLoader::new);

    private PersistentActor<ShardKey> loadActorFromCache(ActorRef actorRef)
        throws ExecutionException
    {
        return actorCache.get(
            actorRef,
            cacheLoaderPool.get().fill(
                persistentActorRepository,
                shardKey,
                actorSystem,
                actorExecutor,
                actorStateUpdateProcessor,
                actorRef
            )
        );
    }

    private final static class CacheLoader implements Callable<PersistentActor<ShardKey>> {

        private PersistentActorRepository persistentActorRepository;
        private ShardKey shardKey;
        private InternalActorSystem actorSystem;
        private ThreadBoundExecutor actorExecutor;
        private ActorStateUpdateProcessor actorStateUpdateProcessor;
        private ActorRef actorRef;

        private CacheLoader fill(
            PersistentActorRepository persistentActorRepository,
            ShardKey shardKey,
            InternalActorSystem actorSystem,
            ThreadBoundExecutor actorExecutor,
            ActorStateUpdateProcessor actorStateUpdateProcessor,
            ActorRef actorRef)
        {
            this.persistentActorRepository = persistentActorRepository;
            this.shardKey = shardKey;
            this.actorSystem = actorSystem;
            this.actorExecutor = actorExecutor;
            this.actorStateUpdateProcessor = actorStateUpdateProcessor;
            this.actorRef = actorRef;
            return this;
        }

        @Override
        public PersistentActor<ShardKey> call() throws Exception {
            PersistentActor<ShardKey> loadedActor =
                persistentActorRepository.get(shardKey, actorRef.getActorId());
            if (loadedActor == null) {
                // @todo: using Spring DataAccesException here, might want to change this or use
                //  in Repository implementation
                throw new EmptyResultDataAccessException(
                    String.format(
                        "Actor [%s] not found in Shard [%s]",
                        actorRef.getActorId(),
                        shardKey
                    ),
                    1
                );
            } else {
                ElasticActor actorInstance = actorSystem.getActorInstance(
                    actorRef,
                    loadedActor.getActorClass()
                );
                actorExecutor.execute(new ActivateActorTask(
                    actorStateUpdateProcessor,
                    persistentActorRepository,
                    loadedActor,
                    actorSystem,
                    actorInstance,
                    actorRef
                ));
                return loadedActor;
            }
        }
    }
}

