package org.elasticsoftware.elasticactors.cluster.tracing;

import org.elasticsoftware.elasticactors.ActorContext;
import org.elasticsoftware.elasticactors.ActorContextHolder;
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.cluster.scheduler.ScheduledMessage;
import org.elasticsoftware.elasticactors.cluster.tasks.ActivateServiceActorTask;
import org.elasticsoftware.elasticactors.cluster.tasks.HandleServiceMessageTask;
import org.elasticsoftware.elasticactors.cluster.tasks.HandleUndeliverableServiceMessageTask;
import org.elasticsoftware.elasticactors.messaging.InternalMessage;
import org.elasticsoftware.elasticactors.state.PersistentActor;
import org.elasticsoftware.elasticactors.tracing.TraceData;
import org.elasticsoftware.elasticactors.tracing.TraceDataHolder;
import org.slf4j.MDC;

public final class TraceContext {

    private static final String UNKNOWN = "UNKNOWN";

    private static final class InternalTraceDataHolder extends TraceDataHolder {

        private static void setTraceData(TraceData traceData) {
            threadTraceData.set(traceData);
        }
    }

    private static final class InternalActorContext extends ActorContextHolder {

        private static ActorContext getCurrent() {
            return threadContext.get();
        }
    }

    public static final String SPAN_ID_HEADER = "X-B3-SpanId";
    public static final String TRACE_ID_HEADER = "X-B3-TraceId";
    public static final String PARENT_SPAN_ID_HEADER = "X-B3-ParentSpanId";

    private TraceContext() {

    }

    public static void enter(InternalMessage internalMessage) {
        ActorContext actorContext = InternalActorContext.getCurrent();
        String receiver = actorContext != null ? asString(actorContext.getSelf()) : null;
        String receiverType;
        if (actorContext instanceof PersistentActor) {
            receiverType = ((PersistentActor<?>) actorContext).getActorClass().getName();
        } else if (actorContext instanceof HandleServiceMessageTask
                || actorContext instanceof HandleUndeliverableServiceMessageTask
             || actorContext instanceof ActivateServiceActorTask) {
            receiverType = "ServiceActor";
        } else if (actorContext != null) {
            receiverType = actorContext.getClass().getName();
        } else {
            receiverType = UNKNOWN;
        }
        enter(
                internalMessage != null ? internalMessage.getPayloadClass() : null,
                internalMessage != null ? asString(internalMessage.getSender()) : null,
                internalMessage != null ? internalMessage.getRealSender() : null,
                internalMessage != null ? internalMessage.getTraceData() : null,
                receiver,
                receiverType);
    }

    public static void enter(ScheduledMessage scheduledMessage) {
        enter(
                scheduledMessage != null ? scheduledMessage.getMessageClass().getName() : null,
                scheduledMessage != null ? asString(scheduledMessage.getSender()) : null,
                scheduledMessage != null ? scheduledMessage.getRealSender() : null,
                scheduledMessage != null ? scheduledMessage.getTraceData() : null,
                scheduledMessage != null ? asString(scheduledMessage.getReceiver()) : null,
                null);
    }

    private static String asString(ActorRef ref) {
        if (ref != null) {
            return ref.toString();
        }
        return null;
    }

    private static void enter(
            String messageType,
            String sender,
            String realSender,
            TraceData traceData,
            String receiver,
            String receiverType) {
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

    private static String getOrUnknown(String receiver) {
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
