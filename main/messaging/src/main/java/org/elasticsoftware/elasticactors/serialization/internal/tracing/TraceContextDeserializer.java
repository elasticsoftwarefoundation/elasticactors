package org.elasticsoftware.elasticactors.serialization.internal.tracing;

import org.elasticsoftware.elasticactors.serialization.protobuf.Messaging;
import org.elasticsoftware.elasticactors.tracing.TraceContext;

import javax.annotation.Nullable;
import java.util.Map;

public final class TraceContextDeserializer {

    private TraceContextDeserializer() {
    }

    @Nullable
    public static TraceContext deserialize(Messaging.TraceContext traceContext) {
        Map<String, String> baggage = traceContext.getBaggageMap();
        TraceContext deserialized = new TraceContext(
            traceContext.hasSpanId() ? traceContext.getSpanId() : "",
            traceContext.hasTraceId() ? traceContext.getTraceId() : "",
            traceContext.hasParentId() ? traceContext.getParentId() : null,
            baggage.isEmpty() ? null : baggage,
            true
        );
        return deserialized.isEmpty() ? null : deserialized;
    }

}
