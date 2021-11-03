package org.elasticsoftware.elasticactors.tracing;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.StringJoiner;
import java.util.concurrent.ThreadLocalRandom;

public final class TraceContext {

    private final String spanId;
    private final String traceId;
    private final String parentId;
    private final Map<String, String> baggage;

    public TraceContext() {
        this(null, null);
    }

    public TraceContext(
        @Nonnull String spanId,
        @Nonnull String traceId,
        @Nullable String parentId,
        @Nullable Map<String, String> baggage)
    {
        this(spanId, traceId, parentId, baggage, false);
    }

    /**
     * Constructor that allows the user to inform if the provided baggage map is immutable,
     * preventing unnecessary copies if so.
     *
     * This is provided for optimization purposes.
     * Passing a map that is not mutable here and changing it afterwards may
     * (and very likely will) corrupt the thread's MDC.
     */
    public TraceContext(
        @Nonnull String spanId,
        @Nonnull String traceId,
        @Nullable String parentId,
        @Nullable Map<String, String> baggage,
        boolean immutableBaggageMap)
    {
        this.spanId = Objects.requireNonNull(spanId);
        this.traceId = Objects.requireNonNull(traceId);
        this.parentId = parentId;
        this.baggage = !immutableBaggageMap && baggage != null
            ? Collections.unmodifiableMap(new HashMap<>(baggage))
            : baggage;
    }

    public TraceContext(@Nullable TraceContext parent) {
        this(parent, parent == null ? null : parent.baggage, true);
    }

    public TraceContext(@Nullable Map<String, String> baggage) {
        this(null, baggage, false);
    }

    public TraceContext(@Nullable TraceContext parent, @Nullable Map<String, String> baggage) {
        this(parent, baggage, false);
    }

    /**
     * Constructor that allows the user to inform if the provided baggage map is immutable,
     * preventing unnecessary copies if so.
     *
     * This is provided for optimization purposes.
     * Passing a map that is not mutable here and changing it afterwards may
     * (and very likely will) corrupt the thread's MDC.
     */
    public TraceContext(
        @Nullable TraceContext parent,
        @Nullable Map<String, String> baggage,
        boolean immutableBaggageMap)
    {
        Random prng = ThreadLocalRandom.current();
        this.spanId = toHexString(prng.nextLong());
        this.traceId = parent == null || parent.getTraceId().isEmpty()
            ? nextTraceIdHigh(prng) + this.spanId
            : parent.getTraceId();
        this.parentId = parent == null || parent.getSpanId().isEmpty()
            ? null
            : parent.getSpanId();
        this.baggage = !immutableBaggageMap && baggage != null
            ? Collections.unmodifiableMap(new HashMap<>(baggage))
            : baggage;
    }

    /**
     * See https://github.com/openzipkin/b3-propagation/issues/6
     */
    @Nonnull
    private static String nextTraceIdHigh(Random prng) {
        long epochSeconds = System.currentTimeMillis() / 1000;
        int random = prng.nextInt();
        long traceIdHigh = (epochSeconds & 0xffffffffL) << 32 | (random & 0xffffffffL);
        return toHexString(traceIdHigh);
    }

    @Nonnull
    private static String toHexString(long number) {
        String numberHex = Long.toHexString(number);
        int zeroes = 16 - numberHex.length();
        if (zeroes == 0) {
            return numberHex;
        }
        StringBuilder sb = new StringBuilder(16);
        for (int i = 0; i < zeroes; i++) {
            sb.append('0');
        }
        sb.append(numberHex);
        return sb.toString();
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
            .add("baggage=" + baggage)
            .toString();
    }

    public boolean isEmpty() {
        return spanId == null || spanId.isEmpty()
                || traceId == null || traceId.isEmpty();
    }

    @Nullable
    public Map<String, String> getBaggage() {
        return baggage;
    }
}
