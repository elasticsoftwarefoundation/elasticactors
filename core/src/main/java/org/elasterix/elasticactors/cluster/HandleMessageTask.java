/*
 * Copyright (c) 2013 Joost van de Wijgerd <jwijgerd@gmail.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.elasterix.elasticactors.cluster;

import org.apache.log4j.Logger;
import org.elasterix.elasticactors.ActorRef;
import org.elasterix.elasticactors.ActorState;
import org.elasterix.elasticactors.ElasticActor;
import org.elasterix.elasticactors.messaging.InternalMessage;
import org.elasterix.elasticactors.serialization.MessageDeserializer;
import org.elasterix.elasticactors.serialization.Serializer;
import org.elasterix.elasticactors.state.PersistentActor;
import org.elasterix.elasticactors.state.PersistentActorRepository;
import org.elasterix.elasticactors.util.SerializationTools;
import org.elasterix.elasticactors.util.concurrent.ThreadBoundRunnable;

import static org.elasterix.elasticactors.util.SerializationTools.deserializeMessage;

/**
 * Task that is responsible for internalMessage deserialization, error handling and state updates
 *
 * @author Joost van de Wijged
 */
public final class HandleMessageTask implements ThreadBoundRunnable<String> {
    private static final Logger log = Logger.getLogger(HandleMessageTask.class);
    private final ActorRef receiverRef;
    private final ElasticActor receiver;
    private final InternalActorSystem actorSystem;
    private final InternalMessage internalMessage;
    private final PersistentActor persistentActor;
    private final PersistentActorRepository persistentActorRepository;

    public HandleMessageTask(InternalActorSystem actorSystem, ElasticActor receiver, InternalMessage internalMessage, PersistentActor persistentActor, PersistentActorRepository persistentActorRepository) {
        this.actorSystem = actorSystem;
        this.receiver = receiver;
        this.internalMessage = internalMessage;
        this.persistentActor = persistentActor;
        this.persistentActorRepository = persistentActorRepository;
        this.receiverRef = internalMessage.getReceiver();
    }


    @Override
    public String getKey() {
        return receiverRef.getActorId();
    }

    @Override
    public void run() {
        try {
            handleMessage(deserializeMessage(actorSystem, internalMessage));
        } catch(Exception e) {
            log.error(String.format("Exception while Deserializing Message class %s in ActorSystem [%s]",
                                    internalMessage.getPayloadClass(),actorSystem.getName()), e);
        }
    }

    private void handleMessage(Object message) {
        ActorState stateBefore = null;
        try {
            // setup the state
            stateBefore = persistentActor.getState(actorSystem.getActorStateDeserializer());
            InternalActorStateContext.setState(stateBefore);
            receiver.onMessage(message,internalMessage.getSender());
        } catch(Exception e) {
            // @todo: handle by sending back a message (if possible)
            log.error(String.format("Exception while handling message for actor [%s]",receiverRef.toString()),e);
        } finally {
            // clear the state
            ActorState stateAfter = InternalActorStateContext.getAndClearState();
            // check if we have state now that needs to be put in the cache
            Serializer<ActorState,byte[]> stateSerializer = actorSystem.getActorStateSerializer();
            try {
                byte[] newActorState = stateSerializer.serialize(stateAfter);
                persistentActor.setSerializedState(newActorState);
                // flush state if it was changed
                if(stateAfter != null) {
                    persistentActorRepository.update(persistentActor.getShardKey(),persistentActor);
                }
            } catch(Exception e) {
                log.error(String.format("Exception while serializing ActorState for actor [%s]",receiverRef.getActorId()),e);
            }
        }
    }
}
