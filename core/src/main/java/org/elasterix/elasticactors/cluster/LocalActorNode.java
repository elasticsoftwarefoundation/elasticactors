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

package org.elasterix.elasticactors.cluster;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.log4j.Logger;
import org.elasterix.elasticactors.*;
import org.elasterix.elasticactors.cluster.tasks.CreateActorTask;
import org.elasterix.elasticactors.cluster.tasks.HandleMessageTask;
import org.elasterix.elasticactors.cluster.tasks.HandleServiceMessageTask;
import org.elasterix.elasticactors.messaging.InternalMessage;
import org.elasterix.elasticactors.messaging.MessageHandlerEventListener;
import org.elasterix.elasticactors.messaging.MessageQueueFactory;
import org.elasterix.elasticactors.messaging.TransientInternalMessage;
import org.elasterix.elasticactors.messaging.internal.CreateActorMessage;
import org.elasterix.elasticactors.messaging.internal.DestroyActorMessage;
import org.elasterix.elasticactors.state.PersistentActor;
import org.elasterix.elasticactors.util.concurrent.ThreadBoundExecutor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Configurable;
import org.springframework.beans.factory.annotation.Qualifier;

import static org.elasterix.elasticactors.util.SerializationTools.deserializeMessage;

/**
 * @author Joost van de Wijgerd
 */
@Configurable
public final class LocalActorNode extends AbstractActorContainer implements ActorNode {
    private static final Logger logger = Logger.getLogger(LocalActorNode.class);
    private final InternalActorSystem actorSystem;
    private final NodeKey nodeKey;
    private ThreadBoundExecutor<String> actorExecutor;
    private Cache<ActorRef,PersistentActor<NodeKey>> actorCache;

    public LocalActorNode(PhysicalNode node,
                          InternalActorSystem actorSystem,
                          ActorRef myRef,
                          MessageQueueFactory messageQueueFactory) {
        super(messageQueueFactory, myRef, node);
        this.actorSystem = actorSystem;
        this.nodeKey = new NodeKey(actorSystem.getName(), node.getId());
    }

    @Override
    public void init() throws Exception {
        super.init();
        //@todo: this cache needs to be parameterized
        this.actorCache = CacheBuilder.newBuilder().build();

    }

    @Override
    public NodeKey getKey() {
        return nodeKey;
    }

    public void sendMessage(ActorRef from, ActorRef to, Object message) throws Exception {
        messageQueue.offer(new TransientInternalMessage(from,to,message));
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
                    actorExecutor.execute(new HandleMessageTask(actorSystem,
                                                                actorInstance,
                                                                internalMessage,
                                                                actor,
                                                                null,
                                                                messageHandlerEventListener));
                } else {
                    // see if it is a service
                    ElasticActor serviceInstance = actorSystem.getServiceInstance(internalMessage.getReceiver());
                    if(serviceInstance != null) {
                        actorExecutor.execute(new HandleServiceMessageTask(actorSystem,
                                                                           internalMessage.getReceiver(),
                                                                           serviceInstance,
                                                                           internalMessage,
                                                                           messageHandlerEventListener));
                    } else {
                        //@todo: send a message undeliverable message
                        // for now just ack it
                        messageHandlerEventListener.onDone(internalMessage);
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

    @Autowired
    public void setActorExecutor(@Qualifier("actorExecutor") ThreadBoundExecutor<String> actorExecutor) {
        this.actorExecutor = actorExecutor;
    }


}

