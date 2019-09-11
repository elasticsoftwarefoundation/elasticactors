package org.elasticsoftware.elasticactors.tracing;

import com.google.common.collect.ImmutableMap;
import org.elasticsoftware.elasticactors.messaging.InternalMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.ServiceLoader;
import java.util.function.Supplier;

final class TracerHolder {

    final static Tracer SINGLETON;

    private static final Logger logger = LoggerFactory.getLogger(Tracer.class);

    static {
        Tracer tracer;
        try {
            Iterator<Tracer> tracerImpls = ServiceLoader.load(Tracer.class).iterator();
            tracer = tracerImpls.hasNext() ? tracerImpls.next() : new NoopTracerImpl();
            if (tracer instanceof NoopTracerImpl) {
                logger.warn("No implementations of {} found. Defaulting to no-op", Tracer.class.getName());
            } else {
                if (tracerImpls.hasNext()) {
                    logger.error("More than one implementation of {} found", Tracer.class.getName());
                }
                logger.info("Loaded {} implementation: {}", Tracer.class.getName(), tracer.getClass().getName());
            }
        } catch (Throwable t) {
            logger.error("Error while instantiating Tracer implementation. Defaulting to no-op", t);
            tracer = new NoopTracerImpl();
        }
        SINGLETON = tracer;
    }

    private static final class NoopTracerImpl implements Tracer {

        @Override
        public void runWithTracing(
                @Nonnull String name,
                @Nullable InternalMessage message,
                @Nonnull Runnable runnable) {
            runnable.run();
        }

        @Override
        public <E extends Throwable> void throwingRunWithTracing(
                @Nonnull String name,
                @Nullable InternalMessage message,
                @Nonnull ThrowingRunnable<E> throwingRunnable) throws E {
            throwingRunnable.run();
        }

        @Override
        public <T> T supplyWithTracing(
                @Nonnull String name,
                @Nullable InternalMessage message,
                @Nonnull Supplier<T> supplier) {
            return supplier.get();
        }

        @Override
        public <T, E extends Throwable> T throwingSupplyWithTracing(
                @Nonnull String name,
                @Nullable InternalMessage message,
                @Nonnull ThrowingSupplier<T, E> throwingSupplier) throws E {
            return throwingSupplier.get();
        }

        @Override
        public void runInCurrentTrace(@Nonnull Runnable runnable) {
            runnable.run();
        }

        @Override
        public <E extends Throwable> void throwingRunInCurrentTrace(
                @Nonnull ThrowingRunnable<E> throwingRunnable) throws E {
            throwingRunnable.run();
        }

        @Override
        public <T> T supplyInCurrentTrace(@Nonnull Supplier<T> supplier) {
            return supplier.get();
        }

        @Override
        public <T, E extends Throwable> T throwingSupplyInCurrentTrace(
                @Nonnull ThrowingSupplier<T, E> throwingSupplier) throws E {
            return throwingSupplier.get();
        }

        @Nullable
        @Override
        public ImmutableMap<String, String> getTraceData() {
            return null;
        }
    }

}
