package org.elasticsoftware.elasticactors.tracing;

import com.google.common.collect.ImmutableMap;
import org.elasticsoftware.elasticactors.messaging.InternalMessage;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.function.Supplier;

public interface Tracer {

    static Tracer get() {
        return TracerHolder.SINGLETON;
    }

    void runWithTracing(
            @Nonnull String name,
            @Nullable InternalMessage message,
            @Nonnull Runnable runnable);

    <E extends Throwable> void throwingRunWithTracing(
            @Nonnull String name,
            @Nullable InternalMessage message,
            @Nonnull ThrowingRunnable<E> throwingRunnable) throws E;

    <T> T supplyWithTracing(
            @Nonnull String name,
            @Nullable InternalMessage message,
            @Nonnull Supplier<T> supplier);

    <T, E extends Throwable> T throwingSupplyWithTracing(
            @Nonnull String name,
            @Nullable InternalMessage message,
            @Nonnull ThrowingSupplier<T, E> throwingSupplier) throws E;

    void runInCurrentTrace(@Nonnull Runnable runnable);

    <E extends Throwable> void throwingRunInCurrentTrace(
            @Nonnull ThrowingRunnable<E> throwingRunnable) throws E;

    <T> T supplyInCurrentTrace(@Nonnull Supplier<T> supplier);

    <T, E extends Throwable> T throwingSupplyInCurrentTrace(
            @Nonnull ThrowingSupplier<T, E> throwingSupplier) throws E;

    @Nullable
    ImmutableMap<String, String> getTraceData();

}
