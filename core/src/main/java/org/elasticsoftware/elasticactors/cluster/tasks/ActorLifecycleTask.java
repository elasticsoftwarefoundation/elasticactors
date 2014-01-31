/*
 * Copyright 2013 - 2014 The Original Authors
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

import org.apache.log4j.Logger;
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.ElasticActor;
import org.elasticsoftware.elasticactors.ShardKey;
import org.elasticsoftware.elasticactors.cluster.InternalActorSystem;
import org.elasticsoftware.elasticactors.messaging.InternalMessage;
import org.elasticsoftware.elasticactors.messaging.MessageHandlerEventListener;
import org.elasticsoftware.elasticactors.state.PersistentActor;
import org.elasticsoftware.elasticactors.state.PersistentActorRepository;
import org.elasticsoftware.elasticactors.util.concurrent.ThreadBoundRunnable;

/**
 * @author Joost van de Wijgerd
 */
public abstract class ActorLifecycleTask implements ThreadBoundRunnable<String> {
    private static final Logger log = Logger.getLogger(ActorLifecycleTask.class);
    private final ActorRef receiverRef;
    private final ElasticActor receiver;
    private final InternalActorSystem actorSystem;
    private final PersistentActor persistentActor;
    private final PersistentActorRepository persistentActorRepository;
    private final InternalMessage internalMessage;
    private final MessageHandlerEventListener messageHandlerEventListener;

    protected ActorLifecycleTask(PersistentActorRepository persistentActorRepository,
                                 PersistentActor persistentActor,
                                 InternalActorSystem actorSystem,
                                 ElasticActor receiver,
                                 ActorRef receiverRef,
                                 MessageHandlerEventListener messageHandlerEventListener,
                                 InternalMessage internalMessage) {
        this.persistentActorRepository = persistentActorRepository;
        this.receiverRef = receiverRef;
        this.persistentActor = persistentActor;
        this.actorSystem = actorSystem;
        this.receiver = receiver;
        this.messageHandlerEventListener = messageHandlerEventListener;
        this.internalMessage = internalMessage;
    }

    @Override
    public final String getKey() {
        return receiverRef.getActorId();
    }

    @Override
    public final void run() {
        // setup the context
        Exception executionException = null;
        InternalActorContext.setContext(persistentActor);
        boolean shouldUpdateState = false;
        try {
            shouldUpdateState = doInActorContext(actorSystem, receiver, receiverRef, internalMessage);
        } catch (Exception e) {
            log.error("Exception in doInActorContext",e);
            executionException = e;
        } finally {
            // clear the state from the thread
            InternalActorContext.getAndClearContext();
            // check if we have state now that needs to be put in the cache
            if (persistentActorRepository != null && persistentActor.getState() != null && shouldUpdateState) {
                try {
                    persistentActorRepository.update((ShardKey) persistentActor.getKey(), persistentActor);
                } catch (Exception e) {
                    log.error(String.format("Exception while serializing ActorState for actor [%s]", receiverRef.getActorId()), e);
                }
            }
            if(messageHandlerEventListener != null) {
                if(executionException == null) {
                    messageHandlerEventListener.onDone(internalMessage);
                } else {
                    messageHandlerEventListener.onError(internalMessage,executionException);
                }
            }
        }
    }

    protected abstract boolean doInActorContext(InternalActorSystem actorSystem,
                                                ElasticActor receiver,
                                                ActorRef receiverRef,
                                                InternalMessage internalMessage);

}
