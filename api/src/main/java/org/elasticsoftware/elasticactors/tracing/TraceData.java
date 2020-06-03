package org.elasticsoftware.elasticactors.tracing;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.concurrent.ThreadLocalRandom;

public final class TraceData {

    private final String spanId;
    private final String traceId;
    private final String parentSpanId;

    public TraceData(@Nonnull String spanId, @Nonnull String traceId, String parentSpanId) {
        this.spanId = Objects.requireNonNull(spanId);
        this.traceId = Objects.requireNonNull(traceId);
        this.parentSpanId = parentSpanId;
    }

    public TraceData(TraceData parent) {
        this.spanId = String.format("%016x", ThreadLocalRandom.current().nextLong());
        this.traceId = parent != null ? parent.getTraceId(): this.spanId;
        this.parentSpanId = parent != null ? parent.getSpanId() : null;
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
        if (!(o instanceof TraceData)) {
            return false;
        }

        TraceData traceData = (TraceData) o;

        if (!spanId.equals(traceData.spanId)) {
            return false;
        }
        if (!traceId.equals(traceData.traceId)) {
            return false;
        }
        return Objects.equals(parentSpanId, traceData.parentSpanId);
    }

    @Override
    public int hashCode() {
        int result = spanId.hashCode();
        result = 31 * result + traceId.hashCode();
        result = 31 * result + (parentSpanId != null ? parentSpanId.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", TraceData.class.getSimpleName() + "[", "]")
                .add("spanId='" + spanId + "'")
                .add("traceId='" + traceId + "'")
                .add("parentSpanId='" + parentSpanId + "'")
                .toString();
    }
}
