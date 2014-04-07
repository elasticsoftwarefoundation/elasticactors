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
import org.elasticsoftware.elasticactors.ActorState;
import org.elasticsoftware.elasticactors.ElasticActor;
import org.elasticsoftware.elasticactors.cluster.InternalActorSystem;
import org.elasticsoftware.elasticactors.messaging.InternalMessage;
import org.elasticsoftware.elasticactors.serialization.SerializationFramework;
import org.elasticsoftware.elasticactors.state.ActorLifecycleStep;
import org.elasticsoftware.elasticactors.state.PersistenceConfig;
import org.elasticsoftware.elasticactors.state.PersistentActor;
import org.elasticsoftware.elasticactors.state.PersistentActorRepository;
import org.elasticsoftware.elasticactors.util.SerializationTools;

import java.util.Arrays;

/**
 * @author Joost van de Wijgerd
 */
public final class ActivateActorTask extends ActorLifecycleTask {
    private static final Logger logger = Logger.getLogger(ActivateActorTask.class);
    private final PersistentActor persistentActor;

    public ActivateActorTask(PersistentActorRepository persistentActorRepository,
                             PersistentActor persistentActor,
                             InternalActorSystem actorSystem,
                             ElasticActor receiver,
                             ActorRef receiverRef) {
        super(persistentActorRepository, persistentActor, actorSystem, receiver, receiverRef, null, null);
        this.persistentActor = persistentActor;
    }

    @Override
    protected boolean doInActorContext(InternalActorSystem actorSystem,
                                       ElasticActor receiver,
                                       ActorRef receiverRef,
                                       InternalMessage internalMessage) {
        boolean overridePersistenceConfig = false;
        // need to deserialize the state here (unless there is none)
        if(persistentActor.getSerializedState() != null) {
            final SerializationFramework framework = SerializationTools.getSerializationFramework(actorSystem.getParent(), receiver.getClass());
            try {
                ActorState actorState = receiver.preActivate(persistentActor.getPreviousActorStateVersion(),
                                                             persistentActor.getCurrentActorStateVersion(),
                                                             persistentActor.getSerializedState(),
                                                             framework);
                if(actorState == null) {
                    actorState = SerializationTools.deserializeActorState(actorSystem.getParent(),receiver.getClass(),persistentActor.getSerializedState());
                } else {
                    overridePersistenceConfig = true;
                }
                // set state and remove bytes
                this.persistentActor.setState(actorState);
                this.persistentActor.setSerializedState(null);
            } catch(Exception e) {
                logger.error("Exception calling preActivate",e);
            }
        }

        try {
            receiver.postActivate(persistentActor.getPreviousActorStateVersion());
        } catch (Exception e) {
            logger.error("Exception calling postActivate",e);
            return false;
        }
        // when the preActivate has a result we should always store the state
        if(!overridePersistenceConfig) {
            // check persistence config (if any)
            PersistenceConfig persistenceConfig = receiver.getClass().getAnnotation(PersistenceConfig.class);
            return persistenceConfig == null || Arrays.asList(persistenceConfig.persistOn()).contains(ActorLifecycleStep.ACTIVATE);
        } else {
            return overridePersistenceConfig;
        }
    }
}
