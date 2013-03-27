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

package org.elasterix.elasticactors.cluster.tasks;

import org.apache.log4j.Logger;
import org.elasterix.elasticactors.ActorRef;
import org.elasterix.elasticactors.ElasticActor;
import org.elasterix.elasticactors.cluster.InternalActorSystem;
import org.elasterix.elasticactors.messaging.InternalMessage;
import org.elasterix.elasticactors.messaging.MessageHandlerEventListener;
import org.elasterix.elasticactors.state.PersistentActor;
import org.elasterix.elasticactors.state.PersistentActorRepository;
import org.elasterix.elasticactors.util.concurrent.ThreadBoundRunnable;

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
        try {
            doInActorContext(actorSystem, receiver, receiverRef, internalMessage);
        } catch (Exception e) {
            // @todo: top level handler
            executionException = e;
        } finally {
            // clear the state from the thread
            InternalActorContext.getAndClearContext();
            // check if we have state now that needs to be put in the cache
            if (persistentActor.getState() != null) {
                try {
                    persistentActorRepository.update(persistentActor.getShardKey(), persistentActor);
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

    protected abstract void doInActorContext(InternalActorSystem actorSystem,
                                             ElasticActor receiver,
                                             ActorRef receiverRef,
                                             InternalMessage internalMessage);

}
