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

package org.elasticsoftware.elasticactors.cluster.tasks.app;

import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.ElasticActor;
import org.elasticsoftware.elasticactors.MessageDeliveryException;
import org.elasticsoftware.elasticactors.MethodActor;
import org.elasticsoftware.elasticactors.cluster.InternalActorSystem;
import org.elasticsoftware.elasticactors.cluster.tasks.ActorLifecycleTask;
import org.elasticsoftware.elasticactors.messaging.InternalMessage;
import org.elasticsoftware.elasticactors.messaging.MessageHandlerEventListener;
import org.elasticsoftware.elasticactors.messaging.reactivestreams.NextMessage;
import org.elasticsoftware.elasticactors.serialization.MessageSerializer;
import org.elasticsoftware.elasticactors.state.ActorStateUpdateProcessor;
import org.elasticsoftware.elasticactors.state.MessageSubscriber;
import org.elasticsoftware.elasticactors.state.PersistentActor;
import org.elasticsoftware.elasticactors.state.PersistentActorRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Set;

import static org.elasticsoftware.elasticactors.util.SerializationTools.deserializeMessage;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Task that is responsible for internalMessage deserialization, error handling and state updates
 *
 * @author Joost van de Wijged
 */
public final class HandleMessageTask extends ActorLifecycleTask {
    private static final Logger log = LoggerFactory.getLogger(HandleMessageTask.class);


    public HandleMessageTask(InternalActorSystem actorSystem,
                             ElasticActor receiver,
                             ActorRef receiverRef,
                             InternalMessage internalMessage,
                             PersistentActor persistentActor,
                             PersistentActorRepository persistentActorRepository,
                             ActorStateUpdateProcessor actorStateUpdateProcessor,
                             MessageHandlerEventListener messageHandlerEventListener,
                             Long serializationWarnThreshold) {
        super(actorStateUpdateProcessor, persistentActorRepository, persistentActor, actorSystem, receiver, receiverRef, messageHandlerEventListener, internalMessage, serializationWarnThreshold);
    }

    public HandleMessageTask(InternalActorSystem actorSystem,
                             ElasticActor receiver,
                             ActorRef receiverRef,
                             InternalMessage internalMessage,
                             PersistentActor persistentActor,
                             PersistentActorRepository persistentActorRepository,
                             ActorStateUpdateProcessor actorStateUpdateProcessor,
                             MessageHandlerEventListener messageHandlerEventListener) {
        super(actorStateUpdateProcessor,persistentActorRepository, persistentActor, actorSystem, receiver, receiverRef, messageHandlerEventListener, internalMessage, null);
    }


    @Override
    protected boolean doInActorContext(InternalActorSystem actorSystem,
                                       ElasticActor receiver,
                                       ActorRef receiverRef,
                                       InternalMessage internalMessage) {
        try {
            Object message = deserializeMessage(actorSystem, internalMessage);
            try {
                if (receiver instanceof MethodActor) {
                    ((MethodActor) receiver).onReceive(
                            internalMessage.getSender(),
                            message,
                            internalMessage.getPayload());
                } else {
                    receiver.onReceive(internalMessage.getSender(), message);
                }
                // reactive streams
                notifySubscribers(internalMessage);
                return shouldUpdateState(receiver, message);
            } catch(MessageDeliveryException e) {
                // see if it is a recoverable exception
                if(!e.isRecoverable()) {
                    log.error("Unrecoverable MessageDeliveryException while handling message for actor [{}]", receiverRef, e);
                }
                // message cannot be sent but state should be updated as the received message did most likely change
                // the state
                return shouldUpdateState(receiver, message);
            } catch (Exception e) {
                ByteBuffer payload = internalMessage.getPayload();
                log.error(
                        "Exception while handling message. "
                                + "Actor [{}]. "
                                + "Message payload: {}",
                        receiverRef,
                        getAsString(payload),
                        e);
                return false;
            }
        } catch (Exception e) {
            ByteBuffer payload = internalMessage.getPayload();
            log.error(
                    "Exception while Deserializing Message class [{}]. "
                            + "ActorSystem [{}]. "
                            + "Message payload: {}",
                    internalMessage.getPayloadClass(),
                    actorSystem.getName(),
                    getAsString(payload),
                    e);
            return false;
        }
    }

    private String getAsString(ByteBuffer payload) {
        return payload != null ? new String(payload.array(), UTF_8) : null;
    }

    private void notifySubscribers(InternalMessage internalMessage) {
        if(persistentActor.getMessageSubscribers() != null) {
            try {
                // todo consider using ActorRefGroup here
                if(persistentActor.getMessageSubscribers().containsKey(internalMessage.getPayloadClass())) {
                    // copy the bytes from the incoming message, discarding possible changes made in onReceive
                    NextMessage nextMessage = new NextMessage(internalMessage.getPayloadClass(), getMessageBytes(internalMessage));
                    ((Set<MessageSubscriber>) persistentActor.getMessageSubscribers().get(internalMessage.getPayloadClass()))
                            .stream().filter(messageSubscriber -> messageSubscriber.getAndDecrement() > 0)
                            .forEach(messageSubscriber -> messageSubscriber.getSubscriberRef().tell(nextMessage, receiverRef));
                }
            } catch(Exception e) {
                log.error("Unexpected exception while forwarding message to Subscribers", e);
            }
        }
    }

    private byte[] getMessageBytes(InternalMessage internalMessage) throws IOException {
        if(internalMessage.getPayload() != null) {
            if(internalMessage.getPayload().hasArray()) {
                return internalMessage.getPayload().array();
            } else {
                // make sure we are at the start of the payload bytes
                internalMessage.getPayload().rewind();
                byte[] messageBytes = new byte[internalMessage.getPayload().remaining()];
                internalMessage.getPayload().get(messageBytes);
                return messageBytes;
            }
        } else {
            // transient message, need to serialize the bytes
            Object message = internalMessage.getPayload(null);
            ByteBuffer messageBytes = ((MessageSerializer<Object>)actorSystem.getSerializer(message.getClass())).serialize(message);
            if(messageBytes.hasArray()) {
                return messageBytes.array();
            } else {
                byte[] bytes = new byte[internalMessage.getPayload().remaining()];
                messageBytes.get(bytes);
                return bytes;
            }
        }
    }

}
