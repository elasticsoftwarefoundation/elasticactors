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
import org.elasticsoftware.elasticactors.state.ActorLifecycleStep;
import org.elasticsoftware.elasticactors.state.ActorStateUpdateProcessor;
import org.elasticsoftware.elasticactors.state.PersistentActor;
import org.elasticsoftware.elasticactors.state.PersistentActorRepository;

/**
 * @author Joost van de Wijgerd
 */
public final class DestroyActorTask extends ActorLifecycleTask {
    private static final Logger logger = LogManager.getLogger(DestroyActorTask.class);
    private final ShardKey shardKey;

    public DestroyActorTask(ActorStateUpdateProcessor actorStateUpdateProcessor,
                            PersistentActorRepository persistentActorRepository,
                            PersistentActor persistentActor,
                            InternalActorSystem actorSystem,
                            ElasticActor receiver,
                            ActorRef receiverRef,
                            InternalMessage createActorMessage,
                            MessageHandlerEventListener messageHandlerEventListener) {
        super(actorStateUpdateProcessor, persistentActorRepository, persistentActor, actorSystem, receiver, receiverRef,messageHandlerEventListener, createActorMessage);
        this.shardKey = (ShardKey) persistentActor.getKey();

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
            // delete entry here to serialize on state updates
            persistentActorRepository.delete(shardKey, receiverRef.getActorId());
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
}
