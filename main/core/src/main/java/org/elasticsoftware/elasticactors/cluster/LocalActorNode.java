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

package org.elasticsoftware.elasticactors.cluster;

import com.google.common.cache.Cache;
import com.google.common.collect.ImmutableList;
import org.elasticsoftware.elasticactors.ActorNode;
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.ElasticActor;
import org.elasticsoftware.elasticactors.NodeKey;
import org.elasticsoftware.elasticactors.PhysicalNode;
import org.elasticsoftware.elasticactors.cache.EvictionListener;
import org.elasticsoftware.elasticactors.cache.NodeActorCacheManager;
import org.elasticsoftware.elasticactors.cluster.logging.LoggingSettings;
import org.elasticsoftware.elasticactors.cluster.metrics.MetricsSettings;
import org.elasticsoftware.elasticactors.cluster.tasks.ActivateServiceActorTask;
import org.elasticsoftware.elasticactors.cluster.tasks.CreateActorTask;
import org.elasticsoftware.elasticactors.cluster.tasks.DestroyActorTask;
import org.elasticsoftware.elasticactors.cluster.tasks.HandleServiceMessageTask;
import org.elasticsoftware.elasticactors.cluster.tasks.HandleUndeliverableServiceMessageTask;
import org.elasticsoftware.elasticactors.concurrent.Expirable;
import org.elasticsoftware.elasticactors.messaging.InternalMessage;
import org.elasticsoftware.elasticactors.messaging.InternalMessageFactory;
import org.elasticsoftware.elasticactors.messaging.MessageHandlerEventListener;
import org.elasticsoftware.elasticactors.messaging.MessageQueueFactory;
import org.elasticsoftware.elasticactors.messaging.MultiMessageHandlerEventListener;
import org.elasticsoftware.elasticactors.messaging.TransientInternalMessage;
import org.elasticsoftware.elasticactors.messaging.internal.ActivateActorMessage;
import org.elasticsoftware.elasticactors.messaging.internal.ActorType;
import org.elasticsoftware.elasticactors.messaging.internal.CreateActorMessage;
import org.elasticsoftware.elasticactors.messaging.internal.DestroyActorMessage;
import org.elasticsoftware.elasticactors.state.PersistentActor;
import org.elasticsoftware.elasticactors.tracing.Traceable;
import org.elasticsoftware.elasticactors.util.concurrent.DaemonThreadFactory;
import org.elasticsoftware.elasticactors.util.concurrent.ThreadBoundExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.time.Clock;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.elasticsoftware.elasticactors.cluster.tasks.ProtocolFactoryFactory.getProtocolFactory;
import static org.elasticsoftware.elasticactors.util.ClassLoadingHelper.getClassHelper;
import static org.elasticsoftware.elasticactors.util.SerializationTools.deserializeMessage;

/**
 * @author Joost van de Wijgerd
 */
public final class LocalActorNode extends AbstractActorContainer implements ActorNode, EvictionListener<PersistentActor<NodeKey>> {

    private final static Logger staticLogger = LoggerFactory.getLogger(LocalActorNode.class);

    private static final Clock SYSTEM_CLOCK = Clock.systemDefaultZone();

    private final InternalActorSystem actorSystem;
    private final NodeKey nodeKey;
    private final ThreadBoundExecutor actorExecutor;
    private final NodeActorCacheManager actorCacheManager;
    private Cache<ActorRef,PersistentActor<NodeKey>> actorCache;
    private final Set<ElasticActor> initializedActors = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private final MetricsSettings metricsSettings;
    private final LoggingSettings loggingSettings;
    private ScheduledExecutorService actorExpirationScheduler;

    public LocalActorNode(
        PhysicalNode node,
        InternalActorSystem actorSystem,
        ActorRef myRef,
        MessageQueueFactory messageQueueFactory,
        ThreadBoundExecutor actorExecutor,
        NodeActorCacheManager actorCacheManager,
        MetricsSettings metricsSettings,
        LoggingSettings loggingSettings)
    {
        super(
            messageQueueFactory,
            myRef,
            node,
            actorSystem.getQueuesPerNode(),
            actorSystem.getMultiQueueHashSeed()
        );
        this.actorSystem = actorSystem;
        this.actorExecutor = actorExecutor;
        this.actorCacheManager = actorCacheManager;
        this.metricsSettings = metricsSettings;
        this.loggingSettings = loggingSettings;
        this.nodeKey = new NodeKey(actorSystem.getName(), node.getId());
    }

    @Override
    protected Logger initLogger() {
        return staticLogger;
    }

    @Override
    public synchronized void init() throws Exception {
        logger.info("Initializing Local Actor Node [{}]", nodeKey);
        this.actorCache = actorCacheManager.create(nodeKey,this);
        long expirationCheckPeriod = actorCacheManager.getExpirationCheckPeriod();
        if (expirationCheckPeriod > 0) {
            this.actorExpirationScheduler = Executors.newSingleThreadScheduledExecutor(
                new DaemonThreadFactory("ACTOR-EXPIRATION-SCHEDULER"));
            this.actorExpirationScheduler.scheduleAtFixedRate(
                this::checkExpiredActors,
                expirationCheckPeriod,
                expirationCheckPeriod,
                TimeUnit.MILLISECONDS
            );
        }
        super.init();
    }

    @Override
    public void destroy() {
        logger.info("Destroying Local Actor Node [{}]", nodeKey);
        super.destroy();
        if (actorExpirationScheduler != null) {
            actorExpirationScheduler.shutdownNow();
        }
        actorCacheManager.destroy(actorCache);
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

    private static String toLoggableString(@Nullable Object object) {
        return object != null
            ? " " + object + "."
            : "";
    }

    @Override
    public NodeKey getKey() {
        return nodeKey;
    }

    @Override
    public void sendMessage(ActorRef from, List<? extends ActorRef> to, Object message) throws Exception {
        InternalMessage internalMessage;
        if (isCreateTempActorMessage(message)) {
            internalMessage = new TransientInternalMessage(
                from,
                ImmutableList.copyOf(to),
                message
            );
        } else {
            internalMessage = InternalMessageFactory.create(from, to, actorSystem, message);
        }
        offerInternalMessage(internalMessage);
    }

    private static boolean isCreateTempActorMessage(Object message) {
        return message instanceof CreateActorMessage
            && ActorType.TEMP.equals(((CreateActorMessage) message).getType());
    }

    @Override
    public void undeliverableMessage(InternalMessage message, ActorRef receiverRef) throws Exception {
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
        if (internalMessage.getReceivers().size() > 1) {
            messageHandlerEventListener =
                new MultiMessageHandlerEventListener(mhel, internalMessage.getReceivers().size());
        }
        for (ActorRef receiverRef : internalMessage.getReceivers()) {
            if(receiverRef.getActorId() != null) {
                try {
                    // load persistent actor from cache or persistent store
                    PersistentActor<NodeKey> actor = getActorFromCacheIfNotExpired(receiverRef);
                    if(actor != null) {
                        // find actor class behind receiver ActorRef
                        ElasticActor actorInstance = actorSystem.getActorInstance(receiverRef, actor.getActorClass());
                        // execute on its own thread
                        if(internalMessage.isUndeliverable()) {
                            actorExecutor.execute(getProtocolFactory(internalMessage)
                                .createHandleUndeliverableMessageTask(
                                    actorSystem,
                                    actorInstance,
                                    receiverRef,
                                    internalMessage,
                                    actor,
                                    null,
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
                                    null,
                                    null,
                                    messageHandlerEventListener,
                                    metricsSettings,
                                    loggingSettings
                                ));
                        }

                    } else {
                        // see if it is a service
                        ElasticActor serviceInstance = actorSystem.getServiceInstance(receiverRef);
                        if(serviceInstance != null) {
                            // due to the fact that Shards get initialized before ServiceActors it can happen that a
                            // message is sent to the ServiceActor instance before it is initialized. If that happens
                            // we'll initialize this Just In Time (and make sure it's only initialized once)
                            if(!initializedActors.contains(serviceInstance)) {
                                actorExecutor.execute(new ActivateServiceActorTask(actorSystem,receiverRef,serviceInstance,null,null));
                                initializedActors.add(serviceInstance);
                            }
                            // ok, now it can handle the message
                            if(!internalMessage.isUndeliverable()) {
                                actorExecutor.execute(new HandleServiceMessageTask(
                                    actorSystem,
                                    receiverRef,
                                    serviceInstance,
                                    internalMessage,
                                    messageHandlerEventListener,
                                    metricsSettings,
                                    loggingSettings
                                ));
                            } else {
                                actorExecutor.execute(new HandleUndeliverableServiceMessageTask(
                                    actorSystem,
                                    receiverRef,
                                    serviceInstance,
                                    internalMessage,
                                    messageHandlerEventListener,
                                    metricsSettings,
                                    loggingSettings
                                ));
                            }
                        } else {
                            handleUndeliverable(internalMessage, receiverRef, messageHandlerEventListener);
                        }
                    }
                } catch(Exception e) {
                    //@todo: let the sender know his message could not be delivered
                    // we ack the message anyway
                    messageHandlerEventListener.onError(internalMessage,e);
                    logger.error("Exception while handling InternalMessage for Actor [{}]; senderRef [{}], messageType [{}] ",receiverRef.getActorId(),internalMessage.getSender(), internalMessage.getPayloadClass(),e);
                }
            } else {
                // the internalMessage is intended for the shard, this means it's about creating or destroying an actor
                try {
                    Object message = deserializeMessage(actorSystem, internalMessage);
                    // check if the actor exists
                    if(message instanceof CreateActorMessage) {
                        CreateActorMessage createActorMessage = (CreateActorMessage) message;
                        ActorRef actorRef = actorSystem.tempActorFor(createActorMessage.getActorId());
                        if(!actorExists(actorRef)) {
                            createActor(
                                actorRef,
                                createActorMessage,
                                internalMessage,
                                messageHandlerEventListener
                            );
                        } else {
                            // ack message anyway
                            messageHandlerEventListener.onDone(internalMessage);
                        }
                    } else if(message instanceof DestroyActorMessage) {
                        DestroyActorMessage destroyActorMessage = (DestroyActorMessage) message;
                        PersistentActor<NodeKey> persistentActor =
                            actorCache.getIfPresent(destroyActorMessage.getActorRef());
                        if(persistentActor != null) {
                            // run the preDestroy and other cleanup
                            destroyActor(persistentActor, internalMessage, messageHandlerEventListener);
                            // remove from cache
                            this.actorCache.invalidate(destroyActorMessage.getActorRef());
                        } else {
                            // do nothing and simply ack message
                            messageHandlerEventListener.onDone(internalMessage);
                        }
                    } else if(message instanceof ActivateActorMessage) {
                        ActivateActorMessage activateActorMessage = (ActivateActorMessage) message;
                        if(activateActorMessage.getActorType() == ActorType.SERVICE) {
                            activateService(activateActorMessage,internalMessage,messageHandlerEventListener);
                        } else {
                            // we don't support activating any other types
                            logger.error("Received ActivateActorMessage for type [{}], ignoring",activateActorMessage.getActorType());
                            // ack the message anyway
                            messageHandlerEventListener.onDone(internalMessage);
                        }
                    } else {
                        // unknown internal message, just ack it (should not happen)
                        messageHandlerEventListener.onDone(internalMessage);
                    }
                } catch(Exception e) {
                    // @todo: determine if this is a recoverable error case or just a programming error
                    messageHandlerEventListener.onError(internalMessage,e);
                    logger.error("Exception while handling InternalMessage for Node [{}]; senderRef [{}], messageType [{}]", nodeKey, internalMessage.getSender(), internalMessage.getPayloadClass(),e);
                }

            }
        }
    }

    private void checkExpiredActors() {
        ConcurrentMap<ActorRef, PersistentActor<NodeKey>> cacheMap = actorCache.asMap();
        if (!cacheMap.isEmpty()) {
            Clock clock = Clock.fixed(SYSTEM_CLOCK.instant(), SYSTEM_CLOCK.getZone());
            for (PersistentActor<NodeKey> actor : cacheMap.values()) {
                if (isExpired(actor, clock)) {
                    invalidate(actor);
                }
            }
        }
    }

    @Nullable
    private PersistentActor<NodeKey> getActorFromCacheIfNotExpired(ActorRef receiverRef) {
        PersistentActor<NodeKey> actor = actorCache.getIfPresent(receiverRef);
        if (isExpired(actor, SYSTEM_CLOCK)) {
            invalidate(actor);
            return null;
        }
        return actor;
    }

    private boolean isExpired(@Nullable PersistentActor<NodeKey> actor, Clock clock) {
        if (actor != null && actor.getState() instanceof Expirable) {
            Expirable expirable = (Expirable) actor.getState();
            return expirable.getExpirationTime() < clock.millis();
        }
        return false;
    }

    private void invalidate(@Nonnull PersistentActor<NodeKey> actor) {
        try {
            // Destroy the actor due to timeout
            actorSystem.stop(actor.getSelf());
            Traceable traceable = actor.getState() instanceof Traceable
                ? (Traceable) actor.getState()
                : null;
            boolean hasTraceData = traceable != null
                && (traceable.getTraceContext() != null
                || traceable.getCreationContext() != null);
            logger.warn(
                "Actor [{}] of type [{}] expired due to timeout."
                    + "{}"
                    + "{}"
                    + "{}",
                actor.getSelf(),
                actor.getActorClass().getName(),
                hasTraceData
                    ? " Actor created with the following contexts in scope:"
                    : "",
                hasTraceData
                    ? toLoggableString(traceable.getCreationContext())
                    : "",
                hasTraceData
                    ? toLoggableString(traceable.getTraceContext())
                    : ""
            );
        } catch (Exception e) {
            logger.error("Could not expire actor [{}]", actor, e);
        }
    }

    private boolean actorExists(ActorRef actorRef) {
        return getActorFromCacheIfNotExpired(actorRef) != null;
    }

    private void createActor(ActorRef ref, CreateActorMessage createMessage,InternalMessage internalMessage, MessageHandlerEventListener messageHandlerEventListener) throws Exception {
        PersistentActor<NodeKey> persistentActor =
            new PersistentActor<>(
                nodeKey,
                actorSystem,
                actorSystem.getConfiguration().getVersion(),
                ref,
                createMessage.getAffinityKey(),
                (Class<? extends ElasticActor>) getClassHelper().forName(createMessage.getActorClass()),
                createMessage.getInitialState()
            );
        actorCache.put(ref,persistentActor);
        // find actor class behind receiver ActorRef
        ElasticActor actorInstance = actorSystem.getActorInstance(ref,persistentActor.getActorClass());
        // call postCreate
        actorExecutor.execute(new CreateActorTask(persistentActor,
                                                  actorSystem,
                                                  actorInstance,
                                                  ref,
                                                  internalMessage,
                                                  messageHandlerEventListener));
    }

    private void activateService(ActivateActorMessage activateActorMessage,InternalMessage internalMessage, MessageHandlerEventListener messageHandlerEventListener) {
        final ElasticActor serviceActor = actorSystem.getConfiguration().getService(activateActorMessage.getActorId());
        // ServiceActor instances can get messages from standard Actor instances before they've been Activated
        // when this happens the ServiceActor is initialized Just in Time. We need to ensure that the Activate logic
        // only runs once
        if(!initializedActors.contains(serviceActor)) {
            ActorRef serviceRef = actorSystem.serviceActorFor(activateActorMessage.getActorId());
            actorExecutor.execute(new ActivateServiceActorTask(actorSystem,serviceRef,serviceActor,internalMessage,messageHandlerEventListener));
            initializedActors.add(serviceActor);
        }
    }


    private void destroyActor(PersistentActor<NodeKey> persistentActor, InternalMessage internalMessage,
                              MessageHandlerEventListener messageHandlerEventListener) throws Exception {
            // find actor class behind receiver ActorRef
            ElasticActor actorInstance = actorSystem.getActorInstance(persistentActor.getSelf(), persistentActor.getActorClass());
            // call preDestroy
            actorExecutor.execute(new DestroyActorTask( persistentActor,
                                                        actorSystem,
                                                        actorInstance,
                                                        persistentActor.getSelf(),
                                                        internalMessage,
                                                        messageHandlerEventListener));
    }

    @Override
    public boolean isLocal() {
        return true;
    }
}

