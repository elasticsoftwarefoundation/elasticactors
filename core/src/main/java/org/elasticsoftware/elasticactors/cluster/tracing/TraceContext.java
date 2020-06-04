package org.elasticsoftware.elasticactors.cluster.tracing;

import org.elasticsoftware.elasticactors.ActorContext;
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.ElasticActor;
import org.elasticsoftware.elasticactors.cluster.scheduler.ScheduledMessage;
import org.elasticsoftware.elasticactors.messaging.InternalMessage;
import org.elasticsoftware.elasticactors.state.PersistentActor;
import org.elasticsoftware.elasticactors.tracing.TraceData;
import org.elasticsoftware.elasticactors.tracing.TraceDataHolder;
import org.slf4j.MDC;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public final class TraceContext {

    private static final String UNKNOWN = "UNKNOWN";

    private static final class InternalTraceDataHolder extends TraceDataHolder {

        private static void setTraceData(TraceData traceData) {
            threadTraceData.set(traceData);
        }
    }

    public static final String SPAN_ID_HEADER = "X-B3-SpanId";
    public static final String TRACE_ID_HEADER = "X-B3-TraceId";
    public static final String PARENT_SPAN_ID_HEADER = "X-B3-ParentSpanId";

    private TraceContext() {

    }

    public static void enter(
            @Nullable ActorContext actorContext,
            @Nullable ElasticActor<?> receivingActor,
            @Nullable InternalMessage internalMessage) {
        String receiver = actorContext != null ? safeToString(actorContext.getSelf()) : null;
        String receiverType = null;
        if (actorContext instanceof PersistentActor) {
            receiverType = safeToString(((PersistentActor<?>) actorContext).getActorClass());
        }
        if (receiverType == null) {
            if (receivingActor != null) {
                receiverType = receivingActor.getClass().getName();
            } else {
                receiverType = UNKNOWN;
            }
        }
        enter(
                internalMessage != null ? internalMessage.getPayloadClass() : null,
                internalMessage != null ? safeToString(internalMessage.getSender()) : null,
                internalMessage != null ? internalMessage.getRealSender() : null,
                internalMessage != null ? internalMessage.getTraceData() : null,
                receiver,
                receiverType);
    }

    public static void enter(@Nullable ScheduledMessage scheduledMessage) {
        enter(
                scheduledMessage != null ? safeToString(scheduledMessage.getMessageClass()) : null,
                scheduledMessage != null ? safeToString(scheduledMessage.getSender()) : null,
                scheduledMessage != null ? scheduledMessage.getRealSender() : null,
                scheduledMessage != null ? scheduledMessage.getTraceData() : null,
                scheduledMessage != null ? safeToString(scheduledMessage.getReceiver()) : null,
                null);
    }

    @Nullable
    private static String safeToString(@Nullable ActorRef ref) {
        if (ref != null) {
            return ref.toString();
        }
        return null;
    }

    @Nullable
    private static String safeToString(@Nullable Class<?> tClass) {
        if (tClass != null) {
            return tClass.getName();
        }
        return null;
    }

    private static void enter(
            @Nullable String messageType,
            @Nullable String sender,
            @Nullable String realSender,
            @Nullable TraceData traceData,
            @Nullable String receiver,
            @Nullable String receiverType) {
        InternalTraceDataHolder.setTraceData(traceData);
        if (traceData != null) {
            MDC.put(SPAN_ID_HEADER, traceData.getSpanId());
            MDC.put(TRACE_ID_HEADER, traceData.getTraceId());
            if (traceData.getParentSpanId() != null) {
                MDC.put(PARENT_SPAN_ID_HEADER, traceData.getParentSpanId());
            }
        }
        MDC.put("messageType", getOrUnknown(messageType));
        MDC.put("sender", getOrUnknown(sender));
        MDC.put("realSender", getOrUnknown(realSender));
        MDC.put("receiver", getOrUnknown(receiver));
        MDC.put("receiverType", getOrUnknown(receiverType));
    }

    @Nonnull
    private static String getOrUnknown(@Nullable String receiver) {
        return receiver != null ? receiver : UNKNOWN;
    }

    public static void leave() {
        InternalTraceDataHolder.setTraceData(null);
        MDC.remove(SPAN_ID_HEADER);
        MDC.remove(TRACE_ID_HEADER);
        MDC.remove(PARENT_SPAN_ID_HEADER);
        MDC.remove("messageType");
        MDC.remove("sender");
        MDC.remove("realSender");
        MDC.remove("receiver");
        MDC.remove("receiverType");
    }

}
