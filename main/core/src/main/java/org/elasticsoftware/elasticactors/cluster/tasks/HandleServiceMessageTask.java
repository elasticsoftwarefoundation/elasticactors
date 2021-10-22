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
import org.elasticsoftware.elasticactors.MethodActor;
import org.elasticsoftware.elasticactors.PersistentSubscription;
import org.elasticsoftware.elasticactors.TypedActor;
import org.elasticsoftware.elasticactors.cluster.InternalActorSystem;
import org.elasticsoftware.elasticactors.messaging.InternalMessage;
import org.elasticsoftware.elasticactors.messaging.MessageHandlerEventListener;
import org.elasticsoftware.elasticactors.messaging.UUIDTools;
import org.elasticsoftware.elasticactors.serialization.Message;
import org.elasticsoftware.elasticactors.serialization.MessageToStringConverter;
import org.elasticsoftware.elasticactors.tracing.MessagingContextManager.MessagingScope;
import org.elasticsoftware.elasticactors.util.SerializationTools;
import org.elasticsoftware.elasticactors.util.concurrent.ThreadBoundRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static org.elasticsoftware.elasticactors.tracing.MessagingContextManager.getManager;
import static org.elasticsoftware.elasticactors.tracing.TracingUtils.shorten;
import static org.elasticsoftware.elasticactors.util.ClassLoadingHelper.getClassHelper;
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

    private final static boolean loggingEnabled =
        SystemPropertiesResolver.getProperty("ea.logging.messaging.enabled", Boolean.class, false);
    private final static boolean metricsEnabled =
        SystemPropertiesResolver.getProperty("ea.metrics.messaging.enabled", Boolean.class, false);
    private final static Long messageDeliveryWarnThreshold =
        SystemPropertiesResolver.getProperty("ea.metrics.messaging.delivery.warn.threshold", Long.class);
    private final static Long messageHandlingWarnThreshold =
        SystemPropertiesResolver.getProperty("ea.metrics.messaging.handling.warn.threshold", Long.class);

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
        this.measurement = isMeasurementEnabled() ? new Measurement(System.nanoTime()) : null;
    }

    private boolean isMeasurementEnabled() {
        if (logger.isTraceEnabled()
            || messageDeliveryWarnThreshold != null
            || messageHandlingWarnThreshold != null)
        {
            return true;
        }
        if (metricsEnabled) {
            try {
                Class<?> messageClass = getClassHelper().forName(internalMessage.getPayloadClass());
                Message messageAnnotation = messageClass.getAnnotation(Message.class);
                if (messageAnnotation != null) {
                    return contains(messageAnnotation.logOnReceive(), Message.LogFeature.TIMING);
                }
            } catch (ClassNotFoundException ignored) {
            }
        }
        return false;
    }

    private static <T> boolean contains(T[] array, T object) {
        for (T current : array) {
            if (current.equals(object)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public ActorRef getSelf() {
        return serviceRef;
    }

    @Nullable
    @Override
    public Class<?> getSelfType() {
        return serviceActor != null ? serviceActor.getClass() : null;
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
    public void run() {
        try (MessagingScope ignored = getManager().enter(this, internalMessage)) {
            runInContext();
        }
    }

    private void runInContext() {
        // measure start of the execution
        checkDeliveryThresholdExceeded(internalMessage);
        if(this.measurement != null) {
            this.measurement.setExecutionStart(System.nanoTime());
        }
        Exception executionException = null;
        InternalActorContext.setContext(this);
        try {
            Object message = deserializeMessage(actorSystem, internalMessage);
            MessageToStringConverter messageToStringConverter = getStringConverter(message);
            if (loggingEnabled) {

            }
            try {
                if (serviceActor instanceof TypedActor) {
                    ((MethodActor) serviceActor).onReceive(
                            internalMessage.getSender(),
                            message,
                            messageToStringConverter);
                } else {
                    serviceActor.onReceive(internalMessage.getSender(), message);
                }
            } catch (Exception e) {
                logException(message, messageToStringConverter, e);
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
            InternalActorContext.clearContext();
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
            if (logger.isTraceEnabled()) {
                logger.trace(
                    "({}) Message of type [{}] with id [{}] for actor [{}] took {} microsecs in queue, {} microsecs to execute, 0 microsecs to serialize and {} microsecs to ack (state update false)",
                    this.getClass().getSimpleName(),
                    internalMessage.getPayloadClass(),
                    internalMessage.getId(),
                    serviceRef.getActorId(),
                    measurement.getQueueDuration(MICROSECONDS),
                    measurement.getExecutionDuration(MICROSECONDS),
                    measurement.getAckDuration(MICROSECONDS));
            }

            if (metricsEnabled) {
                if (messageHandlingWarnThreshold != null) {

                }
            }
        }
    }

    private void logException(
            Object message,
            MessageToStringConverter messageToStringConverter,
            Exception e) {
        if (logger.isErrorEnabled()) {
            Message messageAnnotation = message.getClass().getAnnotation(Message.class);
            if (messageAnnotation != null && messageAnnotation.logBodyOnError()) {
                logger.error(
                        "Exception while handling message of type [{}]. "
                                + "Service [{}]. "
                                + "Sender [{}]. "
                                + "Message payload [{}].",
                        internalMessage.getPayloadClass(),
                        serviceRef,
                        internalMessage.getSender(),
                        serializeToString(message, messageToStringConverter),
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
            MessageToStringConverter messageToStringConverter) {
        if (messageToStringConverter == null) {
            return null;
        }
        try {
            return messageToStringConverter.convert(message);
        } catch (Exception e) {
            logger.error(
                    "Exception thrown while serializing message of type [{}] to String",
                    message.getClass().getName(),
                    e);
            return null;
        }
    }

    private MessageToStringConverter getStringConverter(Object message) {
        try {
            return SerializationTools.getStringConverter(
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

    private void checkDeliveryThresholdExceeded() {
        if (internalMessage != null
            && metricsEnabled
            && messageDeliveryWarnThreshold != null
            && logger.isWarnEnabled())
        {
            long timestamp = UUIDTools.toUnixTimestamp(internalMessage.getId());
            long delay = timestamp - System.currentTimeMillis();
            if (delay > messageDeliveryWarnThreshold) {
                logger.warn(
                    "Message delivery delay of {} ms exceeds the threshold of {} ms. "
                        + "Receivers [{}]. "
                        + "Sender [{}]. "
                        + "Message type [{}]. "
                        + "Message envelope type [{}]. "
                        + "Trace context: [{}]"
                        + "Creation context: [{}]"
                        + "Message payload size (in bytes): {}",
                    delay,
                    messageDeliveryWarnThreshold,
                    internalMessage.getReceivers(),
                    internalMessage.getSender(),
                    shorten(internalMessage.getPayloadClass()),
                    shorten(internalMessage.getClass()),
                    internalMessage.getTraceContext(),
                    internalMessage.getCreationContext(),
                    internalMessage.hasSerializedPayload()
                        ? internalMessage.getPayload().limit()
                        : "N/A"
                );
            }
        }
    }
}
