package org.elasticsoftware.elasticactors.tracing;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.concurrent.ThreadLocalRandom;

public final class TraceContext {

    private final String spanId;
    private final String traceId;
    private final String parentSpanId;

    public TraceContext() {
        this(null);
    }

    public TraceContext(
            @Nonnull String spanId,
            @Nonnull String traceId,
            @Nullable String parentSpanId) {
        this.spanId = Objects.requireNonNull(spanId);
        this.traceId = Objects.requireNonNull(traceId);
        this.parentSpanId = parentSpanId;
    }

    public TraceContext(@Nullable TraceContext parent) {
        this.spanId = String.format("%016x", ThreadLocalRandom.current().nextLong());
        this.traceId = parent == null || parent.getTraceId().trim().isEmpty()
                ? this.spanId
                : parent.getTraceId();
        this.parentSpanId = parent == null || parent.getSpanId().trim().isEmpty()
                ? null
                : parent.getSpanId();
    }

    @Nonnull
    public String getSpanId() {
        return spanId;
    }

    @Nonnull
    public String getTraceId() {
        return traceId;
    }

    @Nullable
    public String getParentSpanId() {
        return parentSpanId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TraceContext)) {
            return false;
        }
        TraceContext that = (TraceContext) o;
        return spanId.equals(that.spanId) &&
                traceId.equals(that.traceId) &&
                Objects.equals(parentSpanId, that.parentSpanId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(spanId, traceId, parentSpanId);
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", TraceContext.class.getSimpleName() + "[", "]")
                .add("spanId='" + spanId + "'")
                .add("traceId='" + traceId + "'")
                .add("parentSpanId='" + parentSpanId + "'")
                .toString();
    }

    public boolean isEmpty() {
        return spanId == null || spanId.trim().isEmpty()
                || traceId == null || traceId.trim().isEmpty();
    }
}
