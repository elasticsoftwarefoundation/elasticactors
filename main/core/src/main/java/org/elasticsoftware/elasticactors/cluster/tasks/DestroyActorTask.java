/*
 * Copyright 2013 - 2025 The Original Authors
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

package org.elasticsoftware.elasticactors.cluster.tasks;

import com.google.common.collect.HashMultimap;
import org.elasticsoftware.elasticactors.ActorLifecycleListener;
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.ActorState;
import org.elasticsoftware.elasticactors.ElasticActor;
import org.elasticsoftware.elasticactors.ShardKey;
import org.elasticsoftware.elasticactors.cluster.InternalActorSystem;
import org.elasticsoftware.elasticactors.messaging.InternalMessage;
import org.elasticsoftware.elasticactors.messaging.MessageHandlerEventListener;
import org.elasticsoftware.elasticactors.messaging.reactivestreams.CompletedMessage;
import org.elasticsoftware.elasticactors.state.ActorLifecycleStep;
import org.elasticsoftware.elasticactors.state.ActorStateUpdateProcessor;
import org.elasticsoftware.elasticactors.state.MessageSubscriber;
import org.elasticsoftware.elasticactors.state.PersistentActor;
import org.elasticsoftware.elasticactors.state.PersistentActorRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.annotation.Nullable;

/**
 * @author Joost van de Wijgerd
 */
public final class DestroyActorTask extends ActorLifecycleTask {
    private static final Logger logger = LoggerFactory.getLogger(DestroyActorTask.class);
    private final ShardKey shardKey;

    public DestroyActorTask(PersistentActor persistentActor,
                            InternalActorSystem actorSystem,
                            ElasticActor receiver,
                            ActorRef receiverRef,
                            InternalMessage internalMessage,
                            MessageHandlerEventListener messageHandlerEventListener) {
        this(null, null, persistentActor, actorSystem, receiver, receiverRef, internalMessage, messageHandlerEventListener);
    }

    public DestroyActorTask(ActorStateUpdateProcessor actorStateUpdateProcessor,PersistentActor persistentActor,
                            InternalActorSystem actorSystem,
                            ElasticActor receiver,
                            ActorRef receiverRef,
                            InternalMessage internalMessage,
                            MessageHandlerEventListener messageHandlerEventListener) {
        this(actorStateUpdateProcessor, null,persistentActor,actorSystem,receiver,receiverRef,internalMessage,messageHandlerEventListener);
    }

    public DestroyActorTask(ActorStateUpdateProcessor actorStateUpdateProcessor,
                            @Nullable PersistentActorRepository persistentActorRepository,
                            PersistentActor persistentActor,
                            InternalActorSystem actorSystem,
                            ElasticActor receiver,
                            ActorRef receiverRef,
                            InternalMessage internalMessage,
                            MessageHandlerEventListener messageHandlerEventListener) {
        super(
            actorStateUpdateProcessor,
            persistentActorRepository,
            persistentActor,
            actorSystem,
            receiver,
            receiverRef,
            messageHandlerEventListener,
            internalMessage,
            null,
            null
        );
        // the shardkey is only needed when there is a persistentActorRepository set
        this.shardKey = (persistentActorRepository != null) ? (ShardKey) persistentActor.getKey() : null;
    }

    @Override
    protected boolean doInActorContext(InternalActorSystem actorSystem,
                                       ElasticActor receiver,
                                       ActorRef receiverRef,
                                       InternalMessage internalMessage) {
        if(logger.isDebugEnabled()) {
            logger.debug("Destroying Actor for ref [{}] of type [{}]",receiverRef,receiver.getClass().getName());
        }
        try {
            // @todo: figure out the destroyer
            receiver.preDestroy(null);
            notifyPublishers();
            notifySubscribers();
            // delete entry here to serialize on state updates
            if (persistentActorRepository != null) {
                persistentActorRepository.delete(shardKey, receiverRef.getActorId());
            }
        } catch (Exception e) {
            logger.error("Exception calling preDestroy",e);
        }
        // never update record (entry has been deleted)
        return false;
    }

    @Override
    protected ActorLifecycleStep executeLifecycleListener(ActorLifecycleListener listener,ActorRef actorRef,ActorState actorState) {
        listener.preDestroy(actorRef,actorState);
        return ActorLifecycleStep.DESTROY;
    }

    @Override
    protected ActorLifecycleStep getLifeCycleStep() {
        return ActorLifecycleStep.DESTROY;
    }

    private void notifySubscribers() {
        HashMultimap<String, MessageSubscriber> messageSubscribers = persistentActor.getMessageSubscribers();
        if(messageSubscribers != null) {
            // make sure to let my subscribers know I will cease to exist
            try {
                messageSubscribers.forEach((messageName, messageSubscriber) ->
                    messageSubscriber.getSubscriberRef().tell(new CompletedMessage(messageName)));
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
