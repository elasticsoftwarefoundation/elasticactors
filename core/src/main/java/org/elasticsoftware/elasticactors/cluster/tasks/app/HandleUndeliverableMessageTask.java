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

package org.elasticsoftware.elasticactors.cluster.tasks.app;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.ElasticActor;
import org.elasticsoftware.elasticactors.MessageDeliveryException;
import org.elasticsoftware.elasticactors.cluster.InternalActorSystem;
import org.elasticsoftware.elasticactors.cluster.tasks.ActorLifecycleTask;
import org.elasticsoftware.elasticactors.messaging.InternalMessage;
import org.elasticsoftware.elasticactors.messaging.MessageHandlerEventListener;
import org.elasticsoftware.elasticactors.state.PersistentActor;
import org.elasticsoftware.elasticactors.state.PersistentActorRepository;

import static java.lang.String.format;
import static org.elasticsoftware.elasticactors.util.SerializationTools.deserializeMessage;

/**
 * Task that is responsible for internalMessage deserialization, error handling and state updates
 *
 * @author Joost van de Wijged
 */
public final class HandleUndeliverableMessageTask extends ActorLifecycleTask {
    private static final Logger log = LogManager.getLogger(HandleUndeliverableMessageTask.class);

    public HandleUndeliverableMessageTask(InternalActorSystem actorSystem,
                                          ElasticActor receiver,
                                          ActorRef receiverRef,
                                          InternalMessage internalMessage,
                                          PersistentActor persistentActor,
                                          PersistentActorRepository persistentActorRepository,
                                          MessageHandlerEventListener messageHandlerEventListener) {
        super(persistentActorRepository, persistentActor, actorSystem, receiver, receiverRef, messageHandlerEventListener, internalMessage);
    }


    protected boolean doInActorContext(InternalActorSystem actorSystem,
                                       ElasticActor receiver,
                                       ActorRef receiverRef,
                                       InternalMessage internalMessage) {
        try {
            Object message = deserializeMessage(actorSystem, internalMessage);
            try {
                receiver.onUndeliverable(internalMessage.getSender(), message);
                return shouldUpdateState(receiver,message);
            } catch(MessageDeliveryException e) {
                // see if it is a recoverable exception
                if(!e.isRecoverable()) {
                    log.error(format("Unrecoverable MessageDeliveryException while handling message for actor [%s]", receiverRef.toString()), e);
                }
                // message cannot be sent but state should be updated as the received message did most likely change
                // the state
                return shouldUpdateState(receiver, message);
            } catch (Exception e) {
                log.error(String.format("Exception while handling undeliverable message for actor [%s]", receiverRef.toString()), e);
                return false;
            }
        } catch (Exception e) {
            log.error(String.format("Exception while Deserializing Message class %s in ActorSystem [%s]",
                    internalMessage.getPayloadClass(), actorSystem.getName()), e);
            return false;
        }
    }
}
