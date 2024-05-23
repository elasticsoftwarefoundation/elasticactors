/*
 * Copyright 2013 - 2024 The Original Authors
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

package org.elasticsoftware.elasticactors.cluster.logging;

import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.ElasticActor;
import org.elasticsoftware.elasticactors.cluster.InternalActorSystem;
import org.elasticsoftware.elasticactors.cluster.metrics.Measurement;
import org.elasticsoftware.elasticactors.cluster.metrics.MetricsSettings;
import org.elasticsoftware.elasticactors.messaging.InternalMessage;
import org.elasticsoftware.elasticactors.messaging.UUIDTools;
import org.elasticsoftware.elasticactors.messaging.reactivestreams.NextMessage;
import org.elasticsoftware.elasticactors.serialization.Message;
import org.elasticsoftware.elasticactors.serialization.MessageToStringConverter;
import org.elasticsoftware.elasticactors.util.SerializationTools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.function.Function;

import static org.elasticsoftware.elasticactors.tracing.TracingUtils.shorten;
import static org.elasticsoftware.elasticactors.util.ArrayUtils.contains;

import static java.util.concurrent.TimeUnit.MICROSECONDS;

public final class MessageLogger {

    private final static Logger logger = LoggerFactory.getLogger(MessageLogger.class);

    private MessageLogger() {
    }

    public static boolean isMeasurementEnabled(
        InternalMessage internalMessage,
        MetricsSettings metricsSettings,
        LoggingSettings loggingSettings,
        Function<InternalMessage, Class<?>> messageClassUnwrapper)
    {
        return logger.isTraceEnabled()
            || metricsSettings.requiresMeasurement(internalMessage)
            || shouldLogTimingForThisMessage(internalMessage, loggingSettings, messageClassUnwrapper);
    }

    private static boolean isLoggingEnabledForMessage(
        InternalMessage internalMessage,
        LoggingSettings loggingSettings)
    {
        return internalMessage != null
            && logger.isInfoEnabled()
            && loggingSettings.isEnabled(internalMessage);
    }

    private static boolean isBasicLoggingEnabledFor(
        Class<?> messageClass,
        LoggingSettings loggingSettings)
    {
        if (messageClass != null) {
            Message.LogFeature[] logFeatures = loggingSettings.processFeatures(messageClass);
            // If CONTENTS or TIMING are enabled, the information convered by BASIC is already
            // being logged.
            return contains(logFeatures, Message.LogFeature.BASIC)
                && !contains(logFeatures, Message.LogFeature.CONTENTS)
                && !contains(logFeatures, Message.LogFeature.TIMING);
        }
        return false;
    }

    private static boolean isTimingLoggingEnabledFor(
        Class<?> messageClass,
        LoggingSettings loggingSettings)
    {
        if (messageClass != null) {
            Message.LogFeature[] logFeatures = loggingSettings.processFeatures(messageClass);
            return contains(logFeatures, Message.LogFeature.TIMING);
        }
        return false;
    }

    private static boolean isContentLoggingEnabledFor(
        Class<?> messageClass,
        LoggingSettings loggingSettings)
    {
        if (messageClass != null) {
            Message.LogFeature[] logFeatures = loggingSettings.processFeatures(messageClass);
            return contains(logFeatures, Message.LogFeature.CONTENTS);
        }
        return false;
    }

    private static boolean shouldLogTimingForThisMessage(
        InternalMessage internalMessage,
        LoggingSettings loggingSettings,
        Function<InternalMessage, Class<?>> messageClassUnwrapper)
    {
        if (isLoggingEnabledForMessage(internalMessage, loggingSettings)) {
            Class<?> messageClass = messageClassUnwrapper.apply(internalMessage);
            return isContentLoggingEnabledFor(messageClass, loggingSettings);
        }
        return false;
    }

    private static boolean isContentLoggingEnabledOnError(Class<?> messageClass) {
        if (messageClass != null) {
            Message message = messageClass.getAnnotation(Message.class);
            if (message != null) {
                return message.logBodyOnError();
            }
        }
        return false;
    }

    public static void logMessageBasicInformation(
        InternalMessage internalMessage,
        LoggingSettings loggingSettings,
        ElasticActor receiver,
        ActorRef receiverRef,
        Function<InternalMessage, Class<?>> messageClassUnwrapper)
    {
        if (isLoggingEnabledForMessage(internalMessage, loggingSettings)) {
            Class<?> messageClass = messageClassUnwrapper.apply(internalMessage);
            if (isBasicLoggingEnabledFor(messageClass, loggingSettings)) {
                logger.info(
                    "Message of type [{}] received by actor [{}] of type [{}], wrapped in [{}]",
                    shorten(messageClass),
                    receiverRef,
                    shorten(receiver.getClass()),
                    shorten(internalMessage.getClass())
                );
            }
        }
    }

    public static void logMessageTimingInformation(
        InternalMessage internalMessage,
        LoggingSettings loggingSettings,
        Measurement measurement,
        ElasticActor receiver,
        ActorRef receiverRef,
        Function<InternalMessage, Class<?>> messageClassUnwrapper)
    {
        if (isLoggingEnabledForMessage(internalMessage, loggingSettings)) {
            Class<?> messageClass = messageClassUnwrapper.apply(internalMessage);
            if (isTimingLoggingEnabledFor(messageClass, loggingSettings)) {
                logger.info(
                    "Message of type [{}] received by actor [{}] of type [{}], wrapped in [{}]. {}",
                    shorten(messageClass),
                    receiverRef,
                    shorten(receiver.getClass()),
                    shorten(internalMessage.getClass()),
                    measurement.summary(MICROSECONDS)
                );
            }
        }
    }

    public static void logMessageContents(
        InternalMessage internalMessage,
        InternalActorSystem internalActorSystem,
        Object message,
        LoggingSettings loggingSettings,
        ElasticActor receiver,
        ActorRef receiverRef,
        Function<InternalMessage, Class<?>> messageClassUnwrapper)
    {
        if (isLoggingEnabledForMessage(internalMessage, loggingSettings)) {
            Class<?> messageClass =
                message != null ? message.getClass() : messageClassUnwrapper.apply(internalMessage);
            if (isContentLoggingEnabledFor(messageClass, loggingSettings)) {
                MessageToStringConverter messageToStringConverter =
                    getMessageToStringConverter(internalActorSystem, messageClass);
                logger.info(
                    "Message of type [{}] received by actor [{}] of type [{}], wrapped in [{}]. Contents: [{}]",
                    shorten(messageClass),
                    receiverRef,
                    shorten(receiver.getClass()),
                    shorten(internalMessage.getClass()),
                    convertToString(
                        message,
                        internalActorSystem,
                        internalMessage,
                        messageToStringConverter
                    )
                );
            }
        }
    }

    public static void logMessageTimingInformationForTraces(
        Class<?> taskClass,
        InternalMessage internalMessage,
        Measurement measurement,
        ElasticActor receiver,
        ActorRef receiverRef)
    {
        if (measurement != null && logger.isTraceEnabled()) {
            logger.trace(
                "[TRACE ({})] Message of type [{}] with id [{}] for actor [{}] of type [{}]. {}",
                taskClass.getSimpleName(),
                (internalMessage != null) ? internalMessage.getPayloadClass() : null,
                (internalMessage != null) ? internalMessage.getId() : null,
                receiverRef,
                shorten(receiver.getClass()),
                measurement.summary(MICROSECONDS)
            );
        }
    }

    public static void checkMessageHandlingThresholdExceeded(
        Class<?> taskClass,
        InternalMessage internalMessage,
        InternalActorSystem internalActorSystem,
        MetricsSettings metricsSettings,
        Measurement measurement,
        ElasticActor receiver,
        ActorRef receiverRef,
        Function<InternalMessage, Class<?>> messageClassUnwrapper)
    {
        if (logger.isWarnEnabled()
            && metricsSettings.isMessageHandlingWarnThresholdEnabled(internalMessage)
            && measurement != null
            && measurement.getTotalDuration(MICROSECONDS)
            > metricsSettings.getMessageHandlingWarnThreshold())
        {
            logDelay(
                "HANDLING",
                taskClass,
                internalMessage,
                internalActorSystem,
                metricsSettings,
                measurement,
                receiver,
                receiverRef,
                messageClassUnwrapper,
                measurement.getTotalDuration(MICROSECONDS)
            );
        }
    }

    public static void checkSerializationThresholdExceeded(
        Class<?> taskClass,
        InternalMessage internalMessage,
        InternalActorSystem internalActorSystem,
        MetricsSettings metricsSettings,
        Measurement measurement,
        ElasticActor receiver,
        ActorRef receiverRef,
        Function<InternalMessage, Class<?>> messageClassUnwrapper)
    {
        if (logger.isWarnEnabled()
            && metricsSettings.isSerializationWarnThresholdEnabled(internalMessage)
            && measurement != null
            && measurement.getSerializationDuration(MICROSECONDS)
            > metricsSettings.getSerializationWarnThreshold())
        {
            logDelay(
                "SERIALIZATION",
                taskClass,
                internalMessage,
                internalActorSystem,
                metricsSettings,
                measurement,
                receiver,
                receiverRef,
                messageClassUnwrapper,
                measurement.getSerializationDuration(MICROSECONDS)
            );
        }
    }

    public static void checkDeliveryThresholdExceeded(
        Class<?> taskClass,
        InternalMessage internalMessage,
        InternalActorSystem internalActorSystem,
        MetricsSettings metricsSettings,
        ElasticActor receiver,
        ActorRef receiverRef,
        Function<InternalMessage, Class<?>> messageClassUnwrapper)
    {
        if (internalMessage != null
            && logger.isWarnEnabled()
            && metricsSettings.isMessageDeliveryWarnThresholdEnabled(internalMessage))
        {
            long timestamp = UUIDTools.toUnixTimestamp(internalMessage.getId());
            long delay = (System.currentTimeMillis() - timestamp) * 1000;
            if (delay > metricsSettings.getMessageDeliveryWarnThreshold()) {
                logDelay(
                    "DELIVERY",
                    taskClass,
                    internalMessage,
                    internalActorSystem,
                    metricsSettings,
                    null,
                    receiver,
                    receiverRef,
                    messageClassUnwrapper,
                    delay
                );
            }
        }
    }

    private static void logDelay(
        String delayType,
        Class<?> taskClass,
        InternalMessage internalMessage,
        InternalActorSystem internalActorSystem,
        MetricsSettings metricsSettings,
        Measurement measurement,
        ElasticActor receiver,
        ActorRef receiverRef,
        Function<InternalMessage, Class<?>> messageClassUnwrapper,
        long delay)
    {
        Class<?> messageClass = messageClassUnwrapper.apply(internalMessage);
        if (isContentLoggingEnabledOnError(messageClass)) {
            MessageToStringConverter messageToStringConverter =
                getMessageToStringConverter(internalActorSystem, messageClass);
            logger.warn(
                "[THRESHOLD EXCEEDED: {} ({})] "
                    + "Delay of {} microsecs exceeds "
                    + "the threshold of {} microsecs. "
                    + "Actor type [{}]. "
                    + "Receiver [{}]. "
                    + "Sender [{}]. "
                    + "Message type [{}]. "
                    + "Message envelope type [{}]. "
                    + "{}"
                    + "{}"
                    + "{}"
                    + "Message payload: [{}].",
                delayType,
                taskClass.getSimpleName(),
                delay,
                metricsSettings.getMessageDeliveryWarnThreshold(),
                shorten(receiver.getClass()),
                receiverRef,
                internalMessage.getSender(),
                shorten(messageClass),
                shorten(internalMessage.getClass()),
                toLoggableString(internalMessage.getTraceContext()),
                toLoggableString(internalMessage.getCreationContext()),
                measurement != null ? measurement.summary(MICROSECONDS) + ". " : "",
                convertToString(
                    null,
                    internalActorSystem,
                    internalMessage,
                    messageToStringConverter
                )
            );
        } else {
            logger.warn(
                "[THRESHOLD EXCEEDED: {} ({})] "
                    + "Delay of {} microsecs exceeds "
                    + "the threshold of {} microsecs. "
                    + "Actor type [{}]. "
                    + "Receiver [{}]. "
                    + "Sender [{}]. "
                    + "Message type [{}]. "
                    + "Message envelope type [{}]. "
                    + "{}"
                    + "{}"
                    + "{}"
                    + "Message payload size: {} bytes",
                delayType,
                taskClass.getSimpleName(),
                delay,
                metricsSettings.getMessageDeliveryWarnThreshold(),
                shorten(receiver.getClass()),
                receiverRef,
                internalMessage.getSender(),
                messageClass != null
                    ? shorten(messageClass)
                    : shorten(internalMessage.getPayloadClass()),
                shorten(internalMessage.getClass()),
                toLoggableString(internalMessage.getTraceContext()),
                toLoggableString(internalMessage.getCreationContext()),
                measurement != null ? measurement.summary(MICROSECONDS) + ". " : "",
                internalMessage.hasSerializedPayload()
                    ? internalMessage.getPayload().limit()
                    : "N/A"
            );
        }
    }

    private static String toLoggableString(Object object) {
        return object != null
            ? object + ". "
            : "";
    }

    public static String convertToString(
        Object message,
        InternalActorSystem actorSystem,
        InternalMessage internalMessage,
        MessageToStringConverter messageToStringConverter)
    {
        if (messageToStringConverter == null) {
            return null;
        }
        try {
            if (NextMessage.class.getName().equals(internalMessage.getPayloadClass())) {
                NextMessage nextMessage = internalMessage.getPayload(actorSystem.getDeserializer(NextMessage.class));
                return messageToStringConverter.convert(ByteBuffer.wrap(nextMessage.getMessageBytes()));
            }
            if (internalMessage.hasSerializedPayload()) {
                return messageToStringConverter.convert(internalMessage.getPayload());
            } else if (message != null) {
                return messageToStringConverter.convert(message);
            } else if (internalMessage.hasPayloadObject()) {
                return messageToStringConverter.convert(internalMessage.getPayload(null));
            }
        } catch (Exception e) {
            logger.error(
                "Exception thrown while serializing message of type [{}] wrapped in a [{}] to String",
                internalMessage.getPayloadClass(),
                internalMessage.getClass().getSimpleName(),
                e
            );
        }
        return "N/A";
    }

    public static MessageToStringConverter getMessageToStringConverter(
        InternalActorSystem actorSystem,
        Class<?> messageClass)
    {
        try {
            return SerializationTools.getStringConverter(actorSystem.getParent(), messageClass);
        } catch (Exception e) {
            logger.error(
                "Unexpected exception resolving message string serializer for type [{}]",
                messageClass.getName(),
                e
            );
            return null;
        }
    }
}
