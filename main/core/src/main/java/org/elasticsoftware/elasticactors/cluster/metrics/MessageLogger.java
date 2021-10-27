package org.elasticsoftware.elasticactors.cluster.metrics;

import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.ElasticActor;
import org.elasticsoftware.elasticactors.cluster.InternalActorSystem;
import org.elasticsoftware.elasticactors.messaging.InternalMessage;
import org.elasticsoftware.elasticactors.messaging.UUIDTools;
import org.elasticsoftware.elasticactors.serialization.Message;
import org.elasticsoftware.elasticactors.serialization.MessageToStringConverter;
import org.elasticsoftware.elasticactors.util.SerializationTools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Function;

import static org.elasticsoftware.elasticactors.tracing.TracingUtils.shorten;

import static java.util.concurrent.TimeUnit.MICROSECONDS;

public final class MessageLogger {

    private final static Logger logger = LoggerFactory.getLogger(MessageLogger.class);

    private MessageLogger() {
    }

    public static boolean isMeasurementEnabled(
        InternalMessage internalMessage,
        MetricsSettings metricsSettings,
        Function<InternalMessage, Class<?>> messageClassUnwrapper)
    {
        return logger.isTraceEnabled()
            || metricsSettings.requiresMeasurement()
            || shouldLogTimingForThisMessage(internalMessage, metricsSettings, messageClassUnwrapper);
    }

    private static boolean isLoggingEnabledForMessage(
        InternalMessage internalMessage,
        MetricsSettings metricsSettings)
    {
        return internalMessage != null
            && logger.isInfoEnabled()
            && metricsSettings.isLoggingEnabled();
    }

    private static boolean shouldLogTimingForThisMessage(
        InternalMessage internalMessage,
        MetricsSettings metricsSettings,
        Function<InternalMessage, Class<?>> messageClassUnwrapper)
    {
        if (isLoggingEnabledForMessage(internalMessage, metricsSettings)) {
            Class<?> messageClass = messageClassUnwrapper.apply(internalMessage);
            return isTimingLoggingEnabledFor(messageClass, metricsSettings);
        }
        return false;
    }

    private static boolean isTimingLoggingEnabledFor(
        Class<?> messageClass,
        MetricsSettings metricsSettings)
    {
        if (messageClass != null) {
            Message.LogFeature[] logFeatures = metricsSettings.processOverrides(messageClass);
            return contains(logFeatures, Message.LogFeature.TIMING);
        }
        return false;
    }

    private static boolean isContentLoggingEnabledFor(
        Class<?> messageClass,
        MetricsSettings metricsSettings)
    {
        if (messageClass != null) {
            Message.LogFeature[] logFeatures = metricsSettings.processOverrides(messageClass);
            return contains(logFeatures, Message.LogFeature.CONTENTS);
        }
        return false;
    }

    private static <T> boolean contains(T[] array, T object) {
        for (T currentObject : array) {
            if (currentObject.equals(object)) {
                return true;
            }
        }
        return false;
    }

    public static void logMessageTimingInformation(
        Class<?> taskClass,
        InternalMessage internalMessage,
        MetricsSettings metricsSettings,
        Measurement measurement,
        ElasticActor receiver,
        ActorRef receiverRef,
        Function<InternalMessage, Class<?>> messageClassUnwrapper)
    {
        if (isLoggingEnabledForMessage(internalMessage, metricsSettings)) {
            Class<?> messageClass = messageClassUnwrapper.apply(internalMessage);
            if (isTimingLoggingEnabledFor(messageClass, metricsSettings)) {
                logger.info(
                    "[TIMING ({})] Message of type [{}] received by actor [{}] of type [{}], wrapped in an [{}]. {}",
                    taskClass.getSimpleName(),
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
        Class<?> taskClass,
        InternalMessage internalMessage,
        InternalActorSystem internalActorSystem,
        Object message,
        MetricsSettings metricsSettings,
        ElasticActor receiver,
        ActorRef receiverRef,
        Function<InternalMessage, Class<?>> messageClassUnwrapper)
    {
        if (isLoggingEnabledForMessage(internalMessage, metricsSettings)) {
            Class<?> messageClass =
                message != null ? message.getClass() : messageClassUnwrapper.apply(internalMessage);
            if (isContentLoggingEnabledFor(messageClass, metricsSettings)) {
                MessageToStringConverter messageToStringConverter =
                    getMessageToStringConverter(internalActorSystem, messageClass);
                logger.info(
                    "[CONTENT ({})] Message of type [{}] received by actor [{}] of type [{}], wrapped in an [{}]. Contents: [{}]",
                    taskClass.getSimpleName(),
                    shorten(messageClass),
                    receiverRef,
                    shorten(receiver.getClass()),
                    shorten(internalMessage.getClass()),
                    convertToString(message, internalMessage, messageToStringConverter)
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
                "### [TRACE ({})] Message of type [{}] with id [{}] for actor [{}] of type [{}]. {}",
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
        MetricsSettings metricsSettings,
        Measurement measurement,
        ElasticActor receiver,
        ActorRef receiverRef)
    {
        if (logger.isWarnEnabled()
            && metricsSettings.isMessageHandlingWarnThresholdEnabled()
            && measurement != null
            && measurement.getTotalDuration(MICROSECONDS)
            > metricsSettings.getMessageHandlingWarnThreshold())
        {
            logger.warn(
                "### [THRESHOLD (HANDLING) ({})] Message of type [{}] with id [{}] for actor"
                    + " [{}] of type [{}] took {} microsecs in total to be handled, exceeding the "
                    + "configured threshold of {} microsecs. {}",
                taskClass.getSimpleName(),
                (internalMessage != null) ? shorten(internalMessage.getPayloadClass()) : "null",
                (internalMessage != null) ? internalMessage.getId() : "null",
                receiverRef,
                shorten(receiver.getClass()),
                measurement.getTotalDuration(MICROSECONDS),
                metricsSettings.getMessageHandlingWarnThreshold(),
                measurement.summary(MICROSECONDS)
            );
        }
    }

    public static void checkSerializationThresholdExceeded(
        Class<?> taskClass,
        InternalMessage internalMessage,
        MetricsSettings metricsSettings,
        Measurement measurement,
        ElasticActor receiver,
        ActorRef receiverRef)
    {
        if (logger.isWarnEnabled()
            && metricsSettings.isSerializationWarnThresholdEnabled()
            && measurement != null
            && measurement.getSerializationDuration(MICROSECONDS)
            > metricsSettings.getSerializationWarnThreshold())
        {
            logger.warn(
                "### [THRESHOLD (SERIALIZATION) ({})] Message of type [{}] with id [{}] triggered "
                    + "serialization for actor [{}] of type [{}] which took {} microsecs, "
                    + "exceeding the configured threshold of {} microsecs. {}",
                taskClass.getSimpleName(),
                (internalMessage != null) ? shorten(internalMessage.getPayloadClass()) : "null",
                (internalMessage != null) ? internalMessage.getId() : "null",
                receiverRef,
                shorten(receiver.getClass()),
                measurement.getSerializationDuration(MICROSECONDS),
                metricsSettings.getSerializationWarnThreshold(),
                measurement.summary(MICROSECONDS)
            );
        }
    }

    public static void checkDeliveryThresholdExceeded(
        Class<?> taskClass,
        InternalMessage internalMessage,
        MetricsSettings metricsSettings,
        ElasticActor receiver,
        ActorRef receiverRef)
    {
        if (internalMessage != null
            && logger.isWarnEnabled()
            && metricsSettings.isMessageDeliveryWarnThresholdEnabled())
        {
            long timestamp = UUIDTools.toUnixTimestamp(internalMessage.getId());
            long delay = (System.currentTimeMillis() - timestamp) * 1000;
            if (delay > metricsSettings.getMessageDeliveryWarnThreshold()) {
                logger.warn(
                    "### [THRESHOLD (DELIVERY) ({})] Message delivery delay of {} microsecs exceeds "
                        + "the threshold of {} microsecs. "
                        + "Actor type [{}]. "
                        + "Receiver [{}]. "
                        + "Sender [{}]. "
                        + "Message type [{}]. "
                        + "Message envelope type [{}]. "
                        + "Trace context: [{}]. "
                        + "Creation context: [{}]. "
                        + "Message payload size: {} bytes",
                    taskClass.getSimpleName(),
                    delay,
                    metricsSettings.getMessageDeliveryWarnThreshold(),
                    shorten(receiver.getClass()),
                    receiverRef,
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

    public static String convertToString(
        Object message,
        InternalMessage internalMessage,
        MessageToStringConverter messageToStringConverter)
    {
        if (messageToStringConverter == null) {
            return null;
        }
        try {
            if (internalMessage.hasSerializedPayload()) {
                return messageToStringConverter.convert(internalMessage.getPayload());
            } else if (message != null) {
                return messageToStringConverter.convert(message);
            }
        } catch (Exception e) {
            logger.error(
                "Exception thrown while serializing message of type [{}] to String",
                message.getClass().getName(),
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
