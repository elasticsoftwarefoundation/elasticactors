package org.elasticsoftware.elasticactors.serialization.internal.tracing;

import org.elasticsoftware.elasticactors.serialization.protobuf.Messaging;
import org.elasticsoftware.elasticactors.tracing.TraceContext;

import javax.annotation.Nullable;

public final class TraceContextDeserializer {

    private TraceContextDeserializer() {
    }

    @Nullable
    public static TraceContext deserialize(Messaging.TraceContext traceContext) {
        TraceContext deserialized = new TraceContext(
                traceContext.getSpanId(),
                traceContext.getTraceId(),
                traceContext.hasParentId() ? traceContext.getParentId() : null);
        return deserialized.isEmpty() ? null : deserialized;
    }

}
