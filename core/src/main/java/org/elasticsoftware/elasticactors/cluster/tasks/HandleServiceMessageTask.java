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

import org.elasticsoftware.elasticactors.ActorContext;
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.ActorState;
import org.elasticsoftware.elasticactors.ActorSystem;
import org.elasticsoftware.elasticactors.ElasticActor;
import org.elasticsoftware.elasticactors.PersistentSubscription;
import org.elasticsoftware.elasticactors.TypedActor;
import org.elasticsoftware.elasticactors.cluster.InternalActorSystem;
import org.elasticsoftware.elasticactors.cluster.tracing.TraceContext;
import org.elasticsoftware.elasticactors.messaging.InternalMessage;
import org.elasticsoftware.elasticactors.messaging.MessageHandlerEventListener;
import org.elasticsoftware.elasticactors.serialization.Message;
import org.elasticsoftware.elasticactors.serialization.MessageToStringSerializer;
import org.elasticsoftware.elasticactors.util.SerializationTools;
import org.elasticsoftware.elasticactors.util.concurrent.ThreadBoundRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static org.elasticsoftware.elasticactors.util.SerializationTools.deserializeMessage;

import static java.util.concurrent.TimeUnit.MICROSECONDS;

/**
 * @author Joost van de Wijgerd
 */
public final class HandleServiceMessageTask implements ThreadBoundRunnable<String>, ActorContext {
    private static final Logger logger = LoggerFactory.getLogger(HandleServiceMessageTask.class);
    private final ActorRef serviceRef;
    private final InternalActorSystem actorSystem;
    private final ElasticActor serviceActor;
    private final InternalMessage internalMessage;
    private final MessageHandlerEventListener messageHandlerEventListener;
    private final Measurement measurement;

    public HandleServiceMessageTask(InternalActorSystem actorSystem,
                                    ActorRef serviceRef,
                                    ElasticActor serviceActor,
                                    InternalMessage internalMessage,
                                    MessageHandlerEventListener messageHandlerEventListener) {
        this.serviceRef = serviceRef;
        this.actorSystem = actorSystem;
        this.serviceActor = serviceActor;
        this.internalMessage = internalMessage;
        this.messageHandlerEventListener = messageHandlerEventListener;
        // only measure when trace is enabled
        this.measurement = logger.isTraceEnabled() ? new Measurement(System.nanoTime()) : null;
    }

    @Override
    public ActorRef getSelf() {
        return serviceRef;
    }

    @Override
    public <T extends ActorState> T getState(Class<T> stateClass) {
        return null;
    }

    @Override
    public void setState(ActorState state) {
        // state not supported
    }

    @Override
    public ActorSystem getActorSystem() {
        return actorSystem;
    }

    @Override
    public String getKey() {
        return serviceRef.getActorId();
    }

    @Override
    public Collection<PersistentSubscription> getSubscriptions() {
        return Collections.emptyList();
    }

    @Override
    public Map<String, Set<ActorRef>> getSubscribers() {
        return Collections.emptyMap();
    }

    @Override
    public final void run() {
        // measure start of the execution
        if(this.measurement != null) {
            this.measurement.setExecutionStart(System.nanoTime());
        }
        Exception executionException = null;
        InternalActorContext.setContext(this);
        TraceContext.enter(this, serviceActor, internalMessage);
        try {
            Object message = deserializeMessage(actorSystem, internalMessage);
            MessageToStringSerializer messageToStringSerializer = getStringSerializer(message);
            try {
                if (serviceActor instanceof TypedActor) {
                    ((TypedActor) serviceActor).onReceive(
                            internalMessage.getSender(),
                            message,
                            messageToStringSerializer);
                } else {
                    serviceActor.onReceive(internalMessage.getSender(), message);
                }
            } catch (Exception e) {
                logException(message, messageToStringSerializer, e);
                executionException = e;
            }
        } catch (Exception e) {
            logger.error(
                    "Exception while Deserializing Message class [{}] in ActorSystem [{}]",
                    internalMessage.getPayloadClass(),
                    actorSystem.getName(),
                    e);
            executionException = e;
        } finally {
            InternalActorContext.getAndClearContext();
            TraceContext.leave();
        }
        // marks the end of the execution path
        if(this.measurement != null) {
            this.measurement.setExecutionEnd(System.nanoTime());
        }
        if(messageHandlerEventListener != null) {
            if(executionException == null) {
                messageHandlerEventListener.onDone(internalMessage);
            } else {
                messageHandlerEventListener.onError(internalMessage,executionException);
            }
            // measure the ack time
            if(this.measurement != null) {
                this.measurement.setAckEnd(System.nanoTime());
            }
        }
        // do some trace logging
        if(this.measurement != null) {
            logger.trace("({}) Message of type [{}] with id [{}] for actor [{}] took {} microsecs in queue, {} microsecs to execute, 0 microsecs to serialize and {} microsecs to ack (state update false)",this.getClass().getSimpleName(),(internalMessage != null) ? internalMessage.getPayloadClass() : "null",(internalMessage != null) ? internalMessage.getId() : "null",serviceRef.getActorId(),measurement.getQueueDuration(MICROSECONDS),measurement.getExecutionDuration(MICROSECONDS),measurement.getAckDuration(MICROSECONDS));
        }
    }

    private void logException(
            Object message,
            MessageToStringSerializer messageToStringSerializer,
            Exception e) {
        if (logger.isErrorEnabled()) {
            Message messageAnnotation = message.getClass().getAnnotation(Message.class);
            if (messageAnnotation != null && messageAnnotation.loggable()) {
                logger.error(
                        "Exception while handling message of type [{}]. "
                                + "Service [{}]. "
                                + "Sender [{}]. "
                                + "Message payload [{}].",
                        internalMessage.getPayloadClass(),
                        serviceRef,
                        internalMessage.getSender(),
                        serializeToString(message, messageToStringSerializer),
                        e);
            } else {
                logger.error(
                        "Exception while handling message of type [{}]. "
                                + "Service [{}]. "
                                + "Sender [{}].",
                        internalMessage.getPayloadClass(),
                        serviceRef,
                        internalMessage.getSender(),
                        e);
            }
        }
    }

    /**
     * Safely serializes the contents of a message to a String
     */
    private String serializeToString(
            Object message,
            MessageToStringSerializer messageToStringSerializer) {
        if (messageToStringSerializer == null) {
            return null;
        }
        try {
            return messageToStringSerializer.serialize(message);
        } catch (Exception e) {
            logger.error(
                    "Exception thrown while serializing message of type [{}] to String",
                    message.getClass().getName(),
                    e);
            return null;
        }
    }

    private MessageToStringSerializer<?> getStringSerializer(Object message) {
        try {
            return SerializationTools.getStringSerializer(
                    actorSystem.getParent(),
                    message.getClass());
        } catch (Exception e) {
            logger.error(
                    "Unexpected exception resolving message string serializer for type [{}]",
                    message.getClass().getName(),
                    e);
            return null;
        }
    }
}
