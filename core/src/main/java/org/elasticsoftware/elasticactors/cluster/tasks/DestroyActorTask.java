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

package org.elasticsoftware.elasticactors.cluster.tasks;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsoftware.elasticactors.*;
import org.elasticsoftware.elasticactors.cluster.InternalActorSystem;
import org.elasticsoftware.elasticactors.messaging.InternalMessage;
import org.elasticsoftware.elasticactors.messaging.MessageHandlerEventListener;
import org.elasticsoftware.elasticactors.messaging.reactivestreams.CompletedMessage;
import org.elasticsoftware.elasticactors.state.MessageSubscriber;
import org.elasticsoftware.elasticactors.state.PersistentActor;
import org.elasticsoftware.elasticactors.state.PersistentActorRepository;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.Set;

/**
 * @author Joost van de Wijgerd
 */
public final class DestroyActorTask extends ActorLifecycleTask {
    private static final Logger logger = LogManager.getLogger(DestroyActorTask.class);
    private final PersistentActorRepository persistentActorRepository;
    private final ShardKey shardKey;

    public DestroyActorTask(PersistentActor persistentActor,
                            InternalActorSystem actorSystem,
                            ElasticActor receiver,
                            ActorRef receiverRef,
                            InternalMessage createActorMessage,
                            MessageHandlerEventListener messageHandlerEventListener) {
        this(null,persistentActor,actorSystem,receiver,receiverRef,createActorMessage,messageHandlerEventListener);
    }

    public DestroyActorTask(@Nullable PersistentActorRepository persistentActorRepository,
                            PersistentActor persistentActor,
                            InternalActorSystem actorSystem,
                            ElasticActor receiver,
                            ActorRef receiverRef,
                            InternalMessage createActorMessage,
                            MessageHandlerEventListener messageHandlerEventListener) {
        super(persistentActorRepository, persistentActor, actorSystem, receiver, receiverRef,messageHandlerEventListener, createActorMessage);
        this.persistentActorRepository = (persistentActorRepository != null) ? persistentActorRepository : null;
        this.shardKey = (persistentActorRepository != null) ? (ShardKey) persistentActor.getKey() : null;
    }

    @Override
    protected boolean doInActorContext(InternalActorSystem actorSystem,
                                       ElasticActor receiver,
                                       ActorRef receiverRef,
                                       InternalMessage internalMessage) {
        if(logger.isDebugEnabled()) {
            logger.debug(String.format("Destroying Actor for ref [%s] of type [%s]",receiverRef.toString(),receiver.getClass().getName()));
        }
        try {
            // @todo: figure out the destroyer
            receiver.preDestroy(null);
            notifyPublishers();
            notifySubscribers();
            // delete entry here to serialize on state updates
            if(persistentActorRepository != null) {
                persistentActorRepository.delete(shardKey, receiverRef.getActorId());
            }
        } catch (Exception e) {
            logger.error("Exception calling preDestroy",e);
        }
        // never update record (entry has been deleted)
        return false;
    }

    @Override
    protected void executeLifecycleListener(ActorLifecycleListener listener,ActorRef actorRef,ActorState actorState) {
        listener.preDestroy(actorRef,actorState);
    }

    private void notifySubscribers() {
        if(persistentActor.getMessageSubscribers() != null) {
            // make sure to let my subscribers know I will cease to exist
            try {
                ((Map<String, Set<MessageSubscriber>>) persistentActor.getMessageSubscribers().asMap())
                        .forEach((messageName, subscribers) -> subscribers
                                .forEach(messageSubscriber -> messageSubscriber.getSubscriberRef()
                                        .tell(new CompletedMessage(messageName))));
            } catch(Exception e) {
                logger.error("Unexpected exception while notifying subscribers", e);
            }
        }

    }

    private void notifyPublishers() {
        // we need to tell our publishers to stop publising.. they will send a completed message
        // that wull fail but this should be no problem
        try {
            persistentActor.cancelAllSubscriptions();
        } catch(Exception e) {
            logger.error("Unexpected Exception while cancelling subscriptions", e);
        }
    }
}
