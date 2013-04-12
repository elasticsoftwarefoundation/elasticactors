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

/**
 * @author Joost van de Wijgerd
 */
public final class DestroyActorTask extends ActorLifecycleTask {
    private static final Logger logger = Logger.getLogger(DestroyActorTask.class);

    public DestroyActorTask(PersistentActor persistentActor,
                            InternalActorSystem actorSystem,
                            ElasticActor receiver,
                            ActorRef receiverRef,
                            InternalMessage createActorMessage,
                            MessageHandlerEventListener messageHandlerEventListener) {
        this(null,persistentActor,actorSystem,receiver,receiverRef,createActorMessage,messageHandlerEventListener);
    }

    public DestroyActorTask(PersistentActorRepository persistentActorRepository,
                            PersistentActor persistentActor,
                            InternalActorSystem actorSystem,
                            ElasticActor receiver,
                            ActorRef receiverRef,
                            InternalMessage createActorMessage,
                            MessageHandlerEventListener messageHandlerEventListener) {
        super(persistentActorRepository, persistentActor, actorSystem, receiver, receiverRef,messageHandlerEventListener, createActorMessage);

    }

    @Override
    protected void doInActorContext(InternalActorSystem actorSystem,
                                    ElasticActor receiver,
                                    ActorRef receiverRef,
                                    InternalMessage internalMessage) {
        if(logger.isDebugEnabled()) {
            logger.debug(String.format("Creating Actor for ref [%s] of type [%s]",receiverRef.toString(),receiver.getClass().getName()));
        }
        try {
            // @todo: figure out the destroyer
            receiver.preDestroy(null);
        } catch (Exception e) {
            logger.error("Exception calling postCreate",e);
        }
    }
}
