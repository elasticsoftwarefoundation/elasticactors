/*
 *   Copyright 2013 - 2019 The Original Authors
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.elasticsoftware.elasticactors.cluster.tasks;

import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.ElasticActor;
import org.elasticsoftware.elasticactors.cluster.InternalActorSystem;
import org.elasticsoftware.elasticactors.messaging.InternalMessage;
import org.elasticsoftware.elasticactors.messaging.MessageHandlerEventListener;
import org.elasticsoftware.elasticactors.state.ActorStateUpdateProcessor;
import org.elasticsoftware.elasticactors.state.PersistentActor;
import org.elasticsoftware.elasticactors.state.PersistentActorRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This will cause the actor state to be saved
 *
 * @author Joost van de Wijgerd
 */
public final class PersistActorTask extends ActorLifecycleTask {
    private static final Logger logger = LoggerFactory.getLogger(PersistActorTask.class);

    public PersistActorTask(ActorStateUpdateProcessor actorStateUpdateProcessor,
                            PersistentActorRepository persistentActorRepository,
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
    }

    @Override
    protected boolean doInActorContext(InternalActorSystem actorSystem,
                                       ElasticActor receiver,
                                       ActorRef receiverRef,
                                       InternalMessage internalMessage) {
        return true;
    }
}
