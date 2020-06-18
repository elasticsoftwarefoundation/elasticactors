package org.elasticsoftware.elasticactors.tracing;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;
import java.util.Random;
import java.util.StringJoiner;
import java.util.concurrent.ThreadLocalRandom;

public final class TraceContext {

    private final String spanId;
    private final String traceId;
    private final String parentId;

    public TraceContext() {
        this(null);
    }

    public TraceContext(
            @Nonnull String spanId,
            @Nonnull String traceId,
            @Nullable String parentId) {
        this.spanId = Objects.requireNonNull(spanId);
        this.traceId = Objects.requireNonNull(traceId);
        this.parentId = parentId;
    }

    public TraceContext(@Nullable TraceContext parent) {
        Random prng = ThreadLocalRandom.current();
        this.spanId = String.format("%016x", prng.nextLong());
        this.traceId = parent == null || parent.getTraceId().trim().isEmpty()
                ? nextTraceIdHigh(prng) + this.spanId
                : parent.getTraceId();
        this.parentId = parent == null || parent.getSpanId().trim().isEmpty()
                ? null
                : parent.getSpanId();
    }

    // https://github.com/openzipkin/b3-propagation/issues/6
    private static String nextTraceIdHigh(Random prng) {
        long epochSeconds = System.currentTimeMillis() / 1000;
        int random = prng.nextInt();
        long traceIdHigh = (epochSeconds & 0xffffffffL) << 32 | (random & 0xffffffffL);
        return String.format("%016x", traceIdHigh);
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
    public String getParentId() {
        return parentId;
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
                Objects.equals(parentId, that.parentId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(spanId, traceId, parentId);
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", TraceContext.class.getSimpleName() + "{", "}")
                .add("spanId='" + spanId + "'")
                .add("traceId='" + traceId + "'")
                .add("parentId='" + parentId + "'")
                .toString();
    }

    public boolean isEmpty() {
        return spanId == null || spanId.trim().isEmpty()
                || traceId == null || traceId.trim().isEmpty();
    }
}
