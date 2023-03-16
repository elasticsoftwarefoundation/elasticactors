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

package org.elasticsoftware.elasticactors.cluster.tasks;

import org.elasticsoftware.elasticactors.ActorLifecycleListener;
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.ActorState;
import org.elasticsoftware.elasticactors.ElasticActor;
import org.elasticsoftware.elasticactors.ShardKey;
import org.elasticsoftware.elasticactors.cluster.InternalActorSystem;
import org.elasticsoftware.elasticactors.messaging.InternalMessage;
import org.elasticsoftware.elasticactors.messaging.MessageHandlerEventListener;
import org.elasticsoftware.elasticactors.state.ActorLifecycleStep;
import org.elasticsoftware.elasticactors.state.ActorStateUpdateProcessor;
import org.elasticsoftware.elasticactors.state.PersistentActor;
import org.elasticsoftware.elasticactors.state.PersistentActorRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

/**
 * @author Joost van de Wijgerd
 */
public final class CreateActorTask extends ActorLifecycleTask {
    private static final Logger logger = LoggerFactory.getLogger(CreateActorTask.class);
    @Nullable
    private final PersistentActor persistentActor;


    public CreateActorTask(PersistentActor persistentActor,
                           InternalActorSystem actorSystem,
                           ElasticActor receiver,
                           ActorRef receiverRef,
                           InternalMessage createActorMessage,
                           MessageHandlerEventListener messageHandlerEventListener) {
        this(null,null,persistentActor,actorSystem,receiver,receiverRef,createActorMessage,messageHandlerEventListener);
    }

    public CreateActorTask(ActorStateUpdateProcessor actorStateUpdateProcessor,
                           PersistentActorRepository persistentActorRepository,
                           PersistentActor persistentActor,
                           InternalActorSystem actorSystem,
                           ElasticActor receiver,
                           ActorRef receiverRef,
                           InternalMessage createActorMessage,
                           MessageHandlerEventListener messageHandlerEventListener) {
        super(
            actorStateUpdateProcessor,
            persistentActorRepository,
            persistentActor,
            actorSystem,
            receiver,
            receiverRef,
            messageHandlerEventListener,
            createActorMessage,
            null,
            null
        );
        this.persistentActor = (persistentActorRepository != null) ? persistentActor : null;
    }

    @Override
    protected boolean doInActorContext(InternalActorSystem actorSystem,
                                       ElasticActor receiver,
                                       ActorRef receiverRef,
                                       InternalMessage internalMessage) {
        if(logger.isDebugEnabled()) {
            logger.debug("Creating Actor for ref [{}] of type [{}]",receiverRef,receiver.getClass().getName());
        }
        try {
            // first update (this is async fire and forget for now)
            if (persistentActor != null) {
                persistentActorRepository.update((ShardKey) persistentActor.getKey(), persistentActor);
            }
            // @todo: somehow figure out the creator

            receiver.postCreate(null);
            receiver.postActivate(null);
        } catch (Exception e) {
            logger.error("Exception calling postCreate",e);
        }
        // check persistence config (if any) -> by default return true
        return shouldUpdateState(receiver, ActorLifecycleStep.CREATE);
    }

    @Override
    protected ActorLifecycleStep executeLifecycleListener(ActorLifecycleListener listener,ActorRef actorRef,ActorState actorState) {
        listener.postCreate(actorRef,actorState);
        listener.postActivate(actorRef,actorState,null);
        return ActorLifecycleStep.CREATE;
    }

    @Override
    protected ActorLifecycleStep getLifeCycleStep() {
        return ActorLifecycleStep.CREATE;
    }
}
