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

import org.elasticsoftware.elasticactors.ActorLifecycleListener;
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.ActorState;
import org.elasticsoftware.elasticactors.ElasticActor;
import org.elasticsoftware.elasticactors.cluster.InternalActorSystem;
import org.elasticsoftware.elasticactors.messaging.InternalMessage;
import org.elasticsoftware.elasticactors.state.ActorLifecycleStep;
import org.elasticsoftware.elasticactors.state.ActorStateUpdateProcessor;
import org.elasticsoftware.elasticactors.state.PersistentActor;
import org.elasticsoftware.elasticactors.state.PersistentActorRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Joost van de Wijgerd
 */
public final class PassivateActorTask extends ActorLifecycleTask {
    private static final Logger logger = LoggerFactory.getLogger(PassivateActorTask.class);

    public PassivateActorTask(ActorStateUpdateProcessor actorStateUpdateProcessor,
                              PersistentActorRepository persistentActorRepository,
                              PersistentActor persistentActor,
                              InternalActorSystem actorSystem,
                              ElasticActor receiver,
                              ActorRef receiverRef) {
        super(
            actorStateUpdateProcessor,
            persistentActorRepository,
            persistentActor,
            actorSystem,
            receiver,
            receiverRef,
            null,
            null,
            null,
            null
        );
    }

    @Override
    protected boolean doInActorContext(InternalActorSystem actorSystem,
                                       ElasticActor receiver,
                                       ActorRef receiverRef,
                                       InternalMessage internalMessage) {
        try {
            receiver.prePassivate();
        } catch (Exception e) {
            logger.error("Exception calling prePassivate",e);
        }
        // check persistence config (if any) -> by default return false
        return shouldUpdateState(receiver,ActorLifecycleStep.PASSIVATE);
    }

    @Override
    protected ActorLifecycleStep executeLifecycleListener(ActorLifecycleListener listener,ActorRef actorRef,ActorState actorState) {
        listener.prePassivate(actorRef,actorState);
        return ActorLifecycleStep.PASSIVATE;
    }

    @Override
    protected ActorLifecycleStep getLifeCycleStep() {
        return ActorLifecycleStep.PASSIVATE;
    }
}
