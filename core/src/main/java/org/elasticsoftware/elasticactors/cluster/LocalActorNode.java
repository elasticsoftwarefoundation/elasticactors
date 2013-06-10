/*
 * Copyright (c) 2013 Joost van de Wijgerd <jwijgerd@gmail.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.elasticsoftware.elasticactors.cluster;

import com.google.common.cache.Cache;
import org.apache.log4j.Logger;
import org.elasticsoftware.elasticactors.*;
import org.elasticsoftware.elasticactors.cache.EvictionListener;
import org.elasticsoftware.elasticactors.cache.NodeActorCacheManager;
import org.elasticsoftware.elasticactors.cluster.tasks.*;
import org.elasticsoftware.elasticactors.messaging.InternalMessage;
import org.elasticsoftware.elasticactors.messaging.MessageHandlerEventListener;
import org.elasticsoftware.elasticactors.messaging.MessageQueueFactory;
import org.elasticsoftware.elasticactors.messaging.TransientInternalMessage;
import org.elasticsoftware.elasticactors.messaging.internal.ActivateActorMessage;
import org.elasticsoftware.elasticactors.messaging.internal.ActorType;
import org.elasticsoftware.elasticactors.messaging.internal.CreateActorMessage;
import org.elasticsoftware.elasticactors.messaging.internal.DestroyActorMessage;
import org.elasticsoftware.elasticactors.serialization.MessageDeserializer;
import org.elasticsoftware.elasticactors.state.PersistentActor;
import org.elasticsoftware.elasticactors.util.concurrent.ThreadBoundExecutor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Configurable;
import org.springframework.beans.factory.annotation.Qualifier;

import static org.elasticsoftware.elasticactors.util.SerializationTools.deserializeMessage;

/**
 * @author Joost van de Wijgerd
 */
@Configurable
public final class LocalActorNode extends AbstractActorContainer implements ActorNode, EvictionListener<PersistentActor<NodeKey>> {
    private static final Logger logger = Logger.getLogger(LocalActorNode.class);
    private final InternalActorSystem actorSystem;
    private final NodeKey nodeKey;
    private ThreadBoundExecutor<String> actorExecutor;
    private final NodeActorCacheManager actorCacheManager;
    private Cache<ActorRef,PersistentActor<NodeKey>> actorCache;

    public LocalActorNode(PhysicalNode node,
                          InternalActorSystem actorSystem,
                          ActorRef myRef,
                          MessageQueueFactory messageQueueFactory,
                          NodeActorCacheManager actorCacheManager) {
        super(messageQueueFactory, myRef, node);
        this.actorSystem = actorSystem;
        this.actorCacheManager = actorCacheManager;
        this.nodeKey = new NodeKey(actorSystem.getName(), node.getId());
    }

    @Override
    public void init() throws Exception {
        super.init();
        this.actorCache = actorCacheManager.create(nodeKey,this);

    }

    @Override
    public void destroy() {
        actorCacheManager.destroy(actorCache);
        super.destroy();
    }

    @Override
    public void onEvicted(PersistentActor<NodeKey> value) {
        // @todo: a temporary actor that gets evicted is actually being destroyed
    }

    @Override
    public NodeKey getKey() {
        return nodeKey;
    }

    public void sendMessage(ActorRef from, ActorRef to, Object message) throws Exception {
        messageQueue.offer(new TransientInternalMessage(from,to,message));
    }

    @Override
    public void undeliverableMessage(InternalMessage message) throws Exception {
        MessageDeserializer messageDeserializer = actorSystem.getDeserializer(Class.forName(message.getPayloadClass()));
        messageQueue.offer(new TransientInternalMessage(message.getReceiver(),
                                                        message.getSender(),
                                                        message.getPayload(messageDeserializer),
                                                        true));
    }

    @Override
    public void handleMessage(final InternalMessage internalMessage,
                              final MessageHandlerEventListener messageHandlerEventListener) {
        final ActorRef receiverRef = internalMessage.getReceiver();
        if(receiverRef.getActorId() != null) {
            try {
                // load persistent actor from cache or persistent store
                PersistentActor<NodeKey> actor = actorCache.getIfPresent(internalMessage.getReceiver());
                if(actor != null) {
                    // find actor class behind receiver ActorRef
                    ElasticActor actorInstance = actorSystem.getActorInstance(internalMessage.getReceiver(),
                            actor.getActorClass());
                    // execute on it's own thread
                    if(internalMessage.isUndeliverable()) {
                        actorExecutor.execute(new HandleUndeliverableMessageTask(actorSystem,
                                                                                 actorInstance,
                                                                                 internalMessage,
                                                                                 actor,
                                                                                 null,
                                                                                 messageHandlerEventListener));
                    } else {
                        actorExecutor.execute(new HandleMessageTask(actorSystem,
                                                                    actorInstance,
                                                                    internalMessage,
                                                                    actor,
                                                                    null,
                                                                    messageHandlerEventListener));
                    }

                } else {
                    // see if it is a service
                    ElasticActor serviceInstance = actorSystem.getServiceInstance(internalMessage.getReceiver());
                    if(serviceInstance != null) {
                        if(!internalMessage.isUndeliverable()) {
                        actorExecutor.execute(new HandleServiceMessageTask(actorSystem,
                                                                           internalMessage.getReceiver(),
                                                                           serviceInstance,
                                                                           internalMessage,
                                                                           messageHandlerEventListener));
                        } else {
                            actorExecutor.execute(new HandleUndeliverableServiceMessageTask(actorSystem,
                                                                                            internalMessage.getReceiver(),
                                                                                            serviceInstance,
                                                                                            internalMessage,
                                                                                            messageHandlerEventListener));
                        }
                    } else {
                        handleUndeliverable(internalMessage,messageHandlerEventListener);
                    }
                }
            } catch(Exception e) {
                //@todo: let the sender know his message could not be delivered
                // we ack the message anyway
                messageHandlerEventListener.onError(internalMessage,e);
                logger.error(String.format("Exception while handling InternalMessage or Actor [%s]",receiverRef.getActorId()),e);
            }
        } else {
            // the internalMessage is intended for the shard, this means it's about creating or destroying an actor
            try {
                Object message = deserializeMessage(actorSystem, internalMessage);
                // check if the actor exists
                if(message instanceof CreateActorMessage) {
                    CreateActorMessage createActorMessage = (CreateActorMessage) message;
                    if(!actorExists(createActorMessage.getActorId())) {
                        createActor(createActorMessage,internalMessage,messageHandlerEventListener);
                    } else {
                        // ack message anyway
                        messageHandlerEventListener.onDone(internalMessage);
                    }
                } else if(message instanceof DestroyActorMessage) {
                    DestroyActorMessage destroyActorMessage = (DestroyActorMessage) message;
                    // remove from cache
                    this.actorCache.invalidate(destroyActorMessage.getActorRef());
                    // ack message
                    messageHandlerEventListener.onDone(internalMessage);
                } else if(message instanceof ActivateActorMessage) {
                    ActivateActorMessage activateActorMessage = (ActivateActorMessage) message;
                    if(activateActorMessage.getActorType() == ActorType.SERVICE) {
                        activateService(activateActorMessage,internalMessage,messageHandlerEventListener);
                    } else {
                        // we don't support activating any other types
                        logger.error(String.format("Received ActivateActorMessage for type [%s], ignoring",activateActorMessage.getActorType()));
                        // ack the message anyway
                        messageHandlerEventListener.onDone(internalMessage);
                    }
                }
            } catch(Exception e) {
                // @todo: determine if this is a recoverable error case or just a programming error
                messageHandlerEventListener.onError(internalMessage,e);
                logger.error(String.format("Exception while handling InternalMessage for Shard [%s]", nodeKey.toString()),e);
            }

        }
    }

    private boolean actorExists(String actorId) {
        return actorCache.getIfPresent(actorId) != null;
    }

    private void createActor(CreateActorMessage createMessage,InternalMessage internalMessage, MessageHandlerEventListener messageHandlerEventListener) throws Exception {
        ActorRef ref = actorSystem.tempActorFor(createMessage.getActorId());
        PersistentActor<NodeKey> persistentActor =
                new PersistentActor<NodeKey>(nodeKey,
                                       actorSystem,
                                       actorSystem.getVersion(),
                                       ref,
                                       (Class<? extends ElasticActor>) Class.forName(createMessage.getActorClass()),
                                       createMessage.getInitialState());
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
        ElasticActor serviceActor = actorSystem.getService(activateActorMessage.getActorId());
        ActorRef serviceRef = actorSystem.serviceActorFor(activateActorMessage.getActorId());

        actorExecutor.execute(new ActivateServiceActorTask(actorSystem,serviceRef,serviceActor,internalMessage,messageHandlerEventListener));
    }

    @Autowired
    public void setActorExecutor(@Qualifier("actorExecutor") ThreadBoundExecutor<String> actorExecutor) {
        this.actorExecutor = actorExecutor;
    }


}

