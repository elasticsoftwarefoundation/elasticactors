package org.elasticsoftware.elasticactors.cluster.tracing;

import org.elasticsoftware.elasticactors.ActorContext;
import org.elasticsoftware.elasticactors.ActorContextHolder;
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.messaging.InternalMessage;
import org.elasticsoftware.elasticactors.tracing.RealSenderData;
import org.elasticsoftware.elasticactors.tracing.TraceData;
import org.elasticsoftware.elasticactors.tracing.TraceDataHolder;
import org.slf4j.MDC;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public final class TraceContext {

    private static final String UNKNOWN = "UNKNOWN";

    private static final class InternalTraceDataHolder extends TraceDataHolder {

        private static TraceData replaceTraceData(TraceData traceData) {
            TraceData current = threadTraceData.get();
            threadTraceData.set(traceData);
            return current;
        }
    }

    private final static class InternalActorContextAccessor extends ActorContextHolder {
        private static ActorContext getCurrent() {
            return threadContext.get();
        }
    }

    private final static ThreadLocal<TraceData> previousTraceData = new ThreadLocal<>();

    public static final String SPAN_ID_HEADER = "X-B3-SpanId";
    public static final String TRACE_ID_HEADER = "X-B3-TraceId";
    public static final String PARENT_SPAN_ID_HEADER = "X-B3-ParentSpanId";

    private TraceContext() {
    }

    public static void enter(@Nullable InternalMessage internalMessage) {
        ActorContext actorContext = InternalActorContextAccessor.getCurrent();
        String receiver = actorContext != null ? safeToString(actorContext.getSelf()) : null;
        String receiverType = actorContext != null ? actorContext.getSelfType() : null;
        enter(
                internalMessage != null ? internalMessage.getPayloadClass() : null,
                internalMessage != null ? safeToString(internalMessage.getSender()) : null,
                internalMessage != null ? internalMessage.getRealSenderData() : null,
                internalMessage != null ? internalMessage.getTraceData() : null,
                receiver,
                receiverType);
    }

    @Nullable
    private static String safeToString(@Nullable ActorRef ref) {
        if (ref != null) {
            return ref.toString();
        }
        return null;
    }

    public static void enterTraceDataOnlyContext(@Nullable TraceData traceData) {
        previousTraceData.set(InternalTraceDataHolder.replaceTraceData(traceData));
    }

    private static void enter(
            @Nullable String messageType,
            @Nullable String sender,
            @Nullable RealSenderData realSenderData,
            @Nullable TraceData traceData,
            @Nullable String receiver,
            @Nullable String receiverType) {
        enterTraceDataOnlyContext(traceData);
        if (traceData != null) {
            MDC.put(SPAN_ID_HEADER, traceData.getSpanId());
            MDC.put(TRACE_ID_HEADER, traceData.getTraceId());
            if (traceData.getParentSpanId() != null) {
                MDC.put(PARENT_SPAN_ID_HEADER, traceData.getParentSpanId());
            }
        }
        MDC.put("messageType", getOrUnknown(messageType));
        MDC.put("sender", getOrUnknown(sender));
        MDC.put(
                "realSender",
                getOrUnknown(realSenderData != null ? realSenderData.getRealSender() : null));
        MDC.put(
                "realSenderType",
                getOrUnknown(realSenderData != null ? realSenderData.getRealSenderType() : null));
        MDC.put("receiver", getOrUnknown(receiver));
        MDC.put("receiverType", getOrUnknown(receiverType));
    }

    @Nonnull
    private static String getOrUnknown(@Nullable String receiver) {
        return receiver != null ? receiver : UNKNOWN;
    }

    public static void leave() {
        leaveTraceDataOnlyContext();
        MDC.remove(SPAN_ID_HEADER);
        MDC.remove(TRACE_ID_HEADER);
        MDC.remove(PARENT_SPAN_ID_HEADER);
        MDC.remove("messageType");
        MDC.remove("sender");
        MDC.remove("realSender");
        MDC.remove("realSenderType");
        MDC.remove("receiver");
        MDC.remove("receiverType");
    }

    public static void leaveTraceDataOnlyContext() {
        InternalTraceDataHolder.replaceTraceData(previousTraceData.get());
        previousTraceData.set(null);
    }

}
