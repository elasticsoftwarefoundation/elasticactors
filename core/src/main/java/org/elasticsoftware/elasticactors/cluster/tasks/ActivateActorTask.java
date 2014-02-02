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
import org.elasticsoftware.elasticactors.cluster.InternalActorSystem;
import org.elasticsoftware.elasticactors.messaging.InternalMessage;
import org.elasticsoftware.elasticactors.state.ActorLifecycleStep;
import org.elasticsoftware.elasticactors.state.PersistenceConfig;
import org.elasticsoftware.elasticactors.state.PersistentActor;
import org.elasticsoftware.elasticactors.state.PersistentActorRepository;

import java.util.Arrays;

/**
 * @author Joost van de Wijgerd
 */
public final class ActivateActorTask extends ActorLifecycleTask {
    private static final Logger logger = Logger.getLogger(ActivateActorTask.class);
    private final String previousActorSystemVersion;

    public ActivateActorTask(PersistentActorRepository persistentActorRepository,
                             PersistentActor persistentActor,
                             InternalActorSystem actorSystem,
                             ElasticActor receiver,
                             ActorRef receiverRef) {
        super(persistentActorRepository, persistentActor, actorSystem, receiver, receiverRef, null, null);
        this.previousActorSystemVersion = persistentActor.getPreviousActorSystemVersion();
    }

    @Override
    protected boolean doInActorContext(InternalActorSystem actorSystem,
                                       ElasticActor receiver,
                                       ActorRef receiverRef,
                                       InternalMessage internalMessage) {
        try {
            // @todo: somehow figure out the creator
            receiver.postActivate(previousActorSystemVersion);
        } catch (Exception e) {
            logger.error("Exception calling postActivate",e);
            return false;
        }
        // check persistence config (if any)
        PersistenceConfig persistenceConfig = receiver.getClass().getAnnotation(PersistenceConfig.class);
        return persistenceConfig == null || Arrays.asList(persistenceConfig.persistOn()).contains(ActorLifecycleStep.CREATE);
    }
}
