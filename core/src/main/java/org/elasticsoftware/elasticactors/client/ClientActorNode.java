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

package org.elasticsoftware.elasticactors.client;

import com.google.common.cache.Cache;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsoftware.elasticactors.*;
import org.elasticsoftware.elasticactors.cache.NodeActorCacheManager;
import org.elasticsoftware.elasticactors.cluster.AbstractActorNode;
import org.elasticsoftware.elasticactors.cluster.MessageSerializationRegistry;
import org.elasticsoftware.elasticactors.messaging.*;
import org.elasticsoftware.elasticactors.messaging.internal.CreateActorMessage;
import org.elasticsoftware.elasticactors.messaging.internal.DestroyActorMessage;
import org.elasticsoftware.elasticactors.state.PersistentActor;
import org.elasticsoftware.elasticactors.util.concurrent.ThreadBoundExecutor;
import org.elasticsoftware.elasticactors.util.concurrent.ThreadBoundRunnable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Configurable;
import org.springframework.beans.factory.annotation.Qualifier;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static java.lang.String.format;
import static org.elasticsoftware.elasticactors.util.SerializationTools.deserializeMessage;

/**
 * @author Joost van de Wijgerd
 */
@Configurable
public final class ClientActorNode extends AbstractActorNode {
    private static final Logger logger = LogManager.getLogger(ClientActorNode.class);
    private final NodeActorCacheManager actorCacheManager;
    protected Cache<ActorRef, PersistentActor<NodeKey>> actorCache;
    private ThreadBoundExecutor actorExecutor;
    private final ConcurrentMap<Class, ElasticActor> actorInstances = new ConcurrentHashMap<>();

    public ClientActorNode(PhysicalNode node,
                           String actorSystemName,
                           MessageSerializationRegistry messageSerializationRegistry,
                           ActorRef myRef,
                           MessageQueueFactory messageQueueFactory,
                           NodeActorCacheManager actorCacheManager) {
        super(messageQueueFactory, myRef, node, actorSystemName, messageSerializationRegistry);
        this.actorCacheManager = actorCacheManager;
    }

    @Override
    public void init() throws Exception {
        this.actorCache = actorCacheManager.create(getKey(),this);
        super.init();
    }

    @Override
    public void destroy() {
        actorCacheManager.destroy(actorCache);
        super.destroy();
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
            if(receiverRef.getActorId() != null) {
                try {
                    // load persistent actor from cache or persistent store
                    PersistentActor<NodeKey> actor = actorCache.getIfPresent(receiverRef);
                    if(actor != null) {
                        // find actor class behind receiver ActorRef
                        final ElasticActor actorInstance = getActorInstance(receiverRef, actor.getActorClass());
                        // execute on it's own thread
                        if(internalMessage.isUndeliverable()) {
                            actorExecutor.execute(new ThreadBoundRunnable<String>() {
                                @Override
                                public void run() {
                                    try {
                                        actorInstance.onUndeliverable(internalMessage.getSender(),
                                                deserializeMessage(messageSerializationRegistry, internalMessage));
                                    } catch (Exception e) {
                                        // @todo: add proper error handling here
                                        logger.error("Unexpected Exception in onUndeliverable handler", e);
                                    }
                                }

                                @Override
                                public String getKey() {
                                    return receiverRef.getActorId();
                                }
                            });
                        } else {
                            actorExecutor.execute(new ThreadBoundRunnable<String>() {
                                @Override
                                public void run() {
                                    try {
                                        actorInstance.onReceive(internalMessage.getSender(),
                                                deserializeMessage(messageSerializationRegistry, internalMessage));
                                    } catch (Exception e) {
                                        // @todo: add proper error handling here
                                        logger.error("Unexpected Exception in onReceive handler", e);
                                    }
                                }

                                @Override
                                public String getKey() {
                                    return receiverRef.getActorId();
                                }
                            });
                        }

                    } else {
                        handleUndeliverable(internalMessage, receiverRef, messageHandlerEventListener);
                    }
                } catch(Exception e) {
                    //@todo: let the sender know his message could not be delivered
                    // we ack the message anyway
                    messageHandlerEventListener.onError(internalMessage,e);
                    logger.error(String.format("Exception while handling InternalMessage for Actor [%s]; senderRef [%s], messageType [%s] ",receiverRef.getActorId(),internalMessage.getSender().toString(), internalMessage.getPayloadClass()),e);
                }
            } else {
                // the internalMessage is intended for the shard, this means it's about creating or destroying an actor
                try {
                    Object message = deserializeMessage(messageSerializationRegistry, internalMessage);
                    // check if the actor exists
                    if(message instanceof CreateActorMessage) {
                        CreateActorMessage createActorMessage = (CreateActorMessage) message;
                        if(actorCache.getIfPresent(createActorMessage.getActorId()) != null) {
                            createActor(createActorMessage,internalMessage,messageHandlerEventListener);
                        } else {
                            // ack message anyway
                            messageHandlerEventListener.onDone(internalMessage);
                        }
                    } else if(message instanceof DestroyActorMessage) {
                        DestroyActorMessage destroyActorMessage = (DestroyActorMessage) message;
                        PersistentActor<NodeKey> persistentActor = this.actorCache.getIfPresent(destroyActorMessage.getActorRef());
                        if(persistentActor != null) {
                            // run the preDestroy and other cleanup
                            destroyActor(persistentActor, internalMessage, messageHandlerEventListener);
                            // remove from cache
                            this.actorCache.invalidate(destroyActorMessage.getActorRef());
                        } else {
                            // do nothing and simply ack message
                            messageHandlerEventListener.onDone(internalMessage);
                        }
                    }  else {
                        // unknown internal message, just ack it (should not happen)
                        messageHandlerEventListener.onDone(internalMessage);
                    }
                } catch(Exception e) {
                    // @todo: determine if this is a recoverable error case or just a programming error
                    messageHandlerEventListener.onError(internalMessage,e);
                    logger.error(String.format("Exception while handling InternalMessage for Shard [%s]; senderRef [%s], messageType [%s]", getKey().toString(), internalMessage.getSender().toString(), internalMessage.getPayloadClass()),e);
                }

            }
        }
    }

    private void createActor(CreateActorMessage createMessage,InternalMessage internalMessage,
                             MessageHandlerEventListener messageHandlerEventListener) throws Exception {
        ActorRef ref = actorSystem.tempActorFor(createMessage.getActorId());
        PersistentActor<NodeKey> persistentActor =
                new PersistentActor<>(getKey(), actorSystem, actorSystem.getConfiguration().getVersion(), ref,
                        createMessage.getAffinityKey(),
                        (Class<? extends ElasticActor>) Class.forName(createMessage.getActorClass()),
                        createMessage.getInitialState());
        actorCache.put(ref,persistentActor);
        // find actor class behind receiver ActorRef
        ElasticActor actorInstance = getActorInstance(ref, persistentActor.getActorClass());
        actorExecutor.execute(new ThreadBoundRunnable<String>() {
            @Override
            public String getKey() {
                return createMessage.getActorId();
            }

            @Override
            public void run() {

            }
        });
    }


    @Autowired
    public void setActorExecutor(@Qualifier("actorExecutor") ThreadBoundExecutor actorExecutor) {
        this.actorExecutor = actorExecutor;
    }

    @Override
    public boolean isLocal() {
        return true;
    }

    private ElasticActor getActorInstance(ActorRef actorRef, Class<? extends ElasticActor> actorClass) {
        // ensure the actor instance is created
        ElasticActor actorInstance = actorInstances.get(actorClass);
        if (actorInstance == null) {
            try {
                actorInstance = actorClass.newInstance();
                ElasticActor existingInstance = actorInstances.putIfAbsent(actorClass, actorInstance);
                return existingInstance == null ? actorInstance : existingInstance;
            } catch (Exception e) {
                logger.error(format("Exception creating actor instance for actorClass [%s]",actorClass.getName()), e);
                return null;
            }
        } else {
            return actorInstance;
        }
    }


}

