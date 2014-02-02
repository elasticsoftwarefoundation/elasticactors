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
import org.elasticsoftware.elasticactors.messaging.MessageHandlerEventListener;
import org.elasticsoftware.elasticactors.state.PersistenceConfig;
import org.elasticsoftware.elasticactors.state.PersistentActor;
import org.elasticsoftware.elasticactors.state.PersistentActorRepository;

import java.util.Arrays;

import static org.elasticsoftware.elasticactors.util.SerializationTools.deserializeMessage;

/**
 * Task that is responsible for internalMessage deserialization, error handling and state updates
 *
 * @author Joost van de Wijged
 */
public final class HandleMessageTask extends ActorLifecycleTask {
    private static final Logger log = Logger.getLogger(HandleMessageTask.class);

    public HandleMessageTask(InternalActorSystem actorSystem,
                             ElasticActor receiver,
                             InternalMessage internalMessage,
                             PersistentActor persistentActor,
                             PersistentActorRepository persistentActorRepository,
                             MessageHandlerEventListener messageHandlerEventListener) {
        super(persistentActorRepository, persistentActor, actorSystem, receiver, internalMessage.getReceiver(), messageHandlerEventListener, internalMessage);
    }


    protected boolean doInActorContext(InternalActorSystem actorSystem,
                                       ElasticActor receiver,
                                       ActorRef receiverRef,
                                       InternalMessage internalMessage) {
        try {
            Object message = deserializeMessage(actorSystem, internalMessage);
            try {
                receiver.onReceive(internalMessage.getSender(), message);
                return shouldUpdateState(receiver.getClass(), message);
            } catch (Exception e) {
                // @todo: handle by sending back a message (if possible)
                log.error(String.format("Exception while handling message for actor [%s]", receiverRef.toString()), e);
                return false;
            }
        } catch (Exception e) {
            log.error(String.format("Exception while Deserializing Message class %s in ActorSystem [%s]",
                    internalMessage.getPayloadClass(), actorSystem.getName()), e);
            return false;
        }
    }

    private boolean shouldUpdateState(Class<? extends  ElasticActor> actorClass, Object message) {
        // look at the annotation on the actor class
        PersistenceConfig persistenceConfig = actorClass.getAnnotation(PersistenceConfig.class);
        if (persistenceConfig != null) {
            // look for not excluded when persist all is on
            if(persistenceConfig.persistOnMessages()) {
                return !Arrays.asList(persistenceConfig.excluded()).contains(message.getClass());
            } else {
                // look for included otherwise
                return Arrays.asList(persistenceConfig.included()).contains(message.getClass());
            }
        } else {
            return true;
        }
    }
}
