/*
 * Copyright 2013 Joost van de Wijgerd
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

package org.elasterix.elasticactors.cluster;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.log4j.Logger;
import org.elasterix.elasticactors.*;
import org.elasterix.elasticactors.cluster.tasks.ActivateActorTask;
import org.elasterix.elasticactors.cluster.tasks.CreateActorTask;
import org.elasterix.elasticactors.cluster.tasks.HandleMessageTask;
import org.elasterix.elasticactors.messaging.*;
import org.elasterix.elasticactors.messaging.internal.CreateActorMessage;
import org.elasterix.elasticactors.serialization.MessageSerializer;
import org.elasterix.elasticactors.state.PersistentActor;
import org.elasterix.elasticactors.state.PersistentActorRepository;
import org.elasterix.elasticactors.util.concurrent.ThreadBoundExecutor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Configurable;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.dao.EmptyResultDataAccessException;

import java.util.concurrent.Callable;

import static org.elasterix.elasticactors.util.SerializationTools.deserializeMessage;

/**
 * @author Joost van de Wijgerd
 */
@Configurable
public final class LocalActorShard implements ActorShard, MessageHandler {
    private static final Logger logger = Logger.getLogger(LocalActorShard.class);
    private final InternalActorSystem actorSystem;
    private final PhysicalNode localNode;
    private final ShardKey shardKey;
    private final MessageQueueFactory messageQueueFactory;
    private MessageQueue messageQueue;
    private ThreadBoundExecutor<String> actorExecutor;
    private Cache<ActorRef,PersistentActor> actorCache;
    private PersistentActorRepository persistentActorRepository;

    public LocalActorShard(PhysicalNode node, InternalActorSystem actorSystem, int shard, MessageQueueFactory messageQueueFactory) {
        this.actorSystem = actorSystem;
        this.localNode = node;
        this.shardKey = new ShardKey(actorSystem.getName(), shard);
        this.messageQueueFactory = messageQueueFactory;
    }

    @Override
    public void init() throws Exception {
        //@todo: this cache needs to be parameterized
        this.actorCache = CacheBuilder.newBuilder().build();
        this.messageQueue = messageQueueFactory.create(shardKey.toString(), this);
    }

    @Override
    public void destroy() {
        // release all resources
        this.messageQueue.destroy();
    }

    @Override
    public PhysicalNode getOwningNode() {
        return localNode;
    }

    @Override
    public ShardKey getKey() {
        return shardKey;
    }

    @Override
    public ActorRef getActorRef() {
        throw new UnsupportedOperationException(String.format("Not meant to be called directly on %s",getClass().getSimpleName()));
    }

    public void sendMessage(ActorRef from, ActorRef to, Object message) throws Exception {
        MessageSerializer messageSerializer = actorSystem.getSerializer(message.getClass());
        messageQueue.offer(new InternalMessageImpl(from, to, messageSerializer.serialize(message), message.getClass().getName()));
    }

    @Override
    public void offerInternalMessage(InternalMessage message) {
        messageQueue.add(message);
    }

    @Override
    public PhysicalNode getPhysicalNode() {
        return localNode;
    }

    @Override
    public void handleMessage(final InternalMessage internalMessage,
                              final MessageHandlerEventListener messageHandlerEventListener) {
        final ActorRef receiverRef = internalMessage.getReceiver();
        if(receiverRef.getActorId() != null) {
            try {
                // load persistent actor from cache or persistent store
                PersistentActor actor = actorCache.get(internalMessage.getReceiver(), new Callable<PersistentActor>() {
                    @Override
                    public PersistentActor call() throws Exception {
                        PersistentActor loadedActor = persistentActorRepository.get(shardKey,receiverRef.getActorId());
                        if(loadedActor == null) {
                            // @todo: using Spring DataAccesException here, might want to change this or use in Repository implementation
                            throw new EmptyResultDataAccessException(String.format("Actor [%s] not found in Shard [%s]",receiverRef.getActorId(),shardKey.toString()),1);
                        } else {
                            ElasticActor actorInstance = actorSystem.getActorInstance(internalMessage.getReceiver(),
                                                                                      loadedActor.getActorClass());
                            actorExecutor.execute(new ActivateActorTask(persistentActorRepository,
                                                                        loadedActor,
                                                                        actorSystem,
                                                                        actorInstance,
                                                                        internalMessage.getReceiver()));
                            return loadedActor;
                        }
                    }
                });

                // find actor class behind receiver ActorRef
                ElasticActor actorInstance = actorSystem.getActorInstance(internalMessage.getReceiver(),
                                                                          actor.getActorClass());
                // execute on it's own thread
                actorExecutor.execute(new HandleMessageTask(actorSystem,
                                                            actorInstance,
                                                            internalMessage,
                                                            actor,
                                                            persistentActorRepository,
                                                            messageHandlerEventListener));
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
                }
            } catch(Exception e) {
                // @todo: determine if this is a recoverable error case or just a programming error
                messageHandlerEventListener.onError(internalMessage,e);
                logger.error(String.format("Exception while handling InternalMessage for Shard [%s]",shardKey.toString()),e);
            }

        }
    }

    private boolean actorExists(String actorId) {
        return actorCache.getIfPresent(actorId) != null || persistentActorRepository.contains(shardKey,actorId);
    }

    private void createActor(CreateActorMessage createMessage,InternalMessage internalMessage, MessageHandlerEventListener messageHandlerEventListener) throws Exception {
        ActorRef ref = actorSystem.actorFor(createMessage.getActorId());
        PersistentActor persistentActor =
                new PersistentActor(shardKey, actorSystem,actorSystem.getVersion(),
                                    ref,
                                    (Class<? extends ElasticActor>) Class.forName(createMessage.getActorClass()),
                                    createMessage.getInitialState());
        persistentActorRepository.update(this.shardKey,persistentActor);
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

    @Autowired
    public void setActorExecutor(@Qualifier("actorExecutor") ThreadBoundExecutor<String> actorExecutor) {
        this.actorExecutor = actorExecutor;
    }

    @Autowired
    public void setPersistentActorRepository(PersistentActorRepository persistentActorRepository) {
        this.persistentActorRepository = persistentActorRepository;
    }
}

