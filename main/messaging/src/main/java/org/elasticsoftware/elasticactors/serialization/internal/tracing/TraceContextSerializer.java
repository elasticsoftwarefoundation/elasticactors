package org.elasticsoftware.elasticactors.serialization.internal.tracing;

import org.elasticsoftware.elasticactors.serialization.protobuf.Messaging;
import org.elasticsoftware.elasticactors.tracing.TraceContext;

import javax.annotation.Nullable;

public final class TraceContextSerializer {

    private TraceContextSerializer() {
    }

    @Nullable
    public static Messaging.TraceContext serialize(@Nullable TraceContext traceContext) {
        if (traceContext != null && !traceContext.isEmpty()) {
            Messaging.TraceContext.Builder serialized = Messaging.TraceContext.newBuilder();
            serialized.setSpanId(traceContext.getSpanId());
            serialized.setTraceId(traceContext.getTraceId());
            if (traceContext.getParentId() != null) {
                serialized.setParentId(traceContext.getParentId());
            }
            if (traceContext.getBaggage() != null && !traceContext.getBaggage().isEmpty()) {
                serialized.putAllBaggage(traceContext.getBaggage());
            }
            return serialized.build();
        }
        return null;
    }

}
