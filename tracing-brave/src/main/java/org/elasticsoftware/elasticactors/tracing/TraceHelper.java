/*
 *   Copyright 2013 - 2019 The Original Authors
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.elasticsoftware.elasticactors.tracing;

import brave.ScopedSpan;
import brave.Span;
import brave.Tracing;
import brave.propagation.Propagation.Getter;
import brave.propagation.Propagation.Setter;
import brave.propagation.TraceContext;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import org.elasticsoftware.elasticactors.messaging.InternalMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.concurrent.Callable;
import java.util.function.Supplier;

public final class TraceHelper {

    private static final Logger logger = LoggerFactory.getLogger(TraceHelper.class);

    private static final Getter<InternalMessage, String> GETTER =
            (c, k) -> c.getTraceData() != null ? c.getTraceData().get(k) : null;
    private static final Setter<Builder<String, String>, String> SETTER = Builder::put;

    static Tracing tracing;

    public static void runWithTracing(@Nonnull String name, @Nullable InternalMessage message, @Nonnull Runnable runnable) {
        if (tracing != null) {
            ScopedSpan span = createScopedSpan(name, message);
            try {
                runnable.run();
            } catch (Throwable t) {
                span.error(t);
                throw t;
            } finally {
                span.finish();
            }
        } else {
            logger.warn("Attempted to run task '{}' with tracing before initialization of Tracing bean", name);
            runnable.run();
        }
    }

    public static <T> T callWithTracing(@Nonnull String name, @Nullable InternalMessage message, @Nonnull Callable<T> callable) throws Exception {
        if (tracing != null) {
            ScopedSpan span = createScopedSpan(name, message);
            try {
                return callable.call();
            } catch (Throwable t) {
                span.error(t);
                throw t;
            } finally {
                span.finish();
            }
        } else {
            logger.warn("Attempted to call task '{}' with tracing before initialization of Tracing bean", name);
            return callable.call();
        }
    }

    public static <T> T supplyWithTracing(@Nonnull String name, @Nullable InternalMessage message, @Nonnull Supplier<T> supplier) {
        if (tracing != null) {
            ScopedSpan span = createScopedSpan(name, message);
            try {
                return supplier.get();
            } catch (Throwable t) {
                span.error(t);
                throw t;
            } finally {
                span.finish();
            }
        } else {
            logger.warn("Attempted to run supplier task '{}' with tracing before initialization of Tracing bean", name);
            return supplier.get();
        }
    }

    public static void throwingRunWithTracing(@Nonnull String name, @Nullable InternalMessage message, @Nonnull ThrowingRunnable throwingRunnable) throws Exception {
        if (tracing != null) {
            ScopedSpan span = createScopedSpan(name, message);
            try {
                throwingRunnable.run();
            } catch (Throwable t) {
                span.error(t);
                throw t;
            } finally {
                span.finish();
            }
        } else {
            logger.warn("Attempted to run throwing runnable task '{}' with tracing before initialization of Tracing bean", name);
            throwingRunnable.run();
        }
    }

    public static void runWithTracing(@Nonnull String name, @Nonnull Runnable runnable) {
        runWithTracing(name, null, runnable);
    }

    public static <T> T callWithTracing(@Nonnull String name, @Nonnull Callable<T> callable) throws Exception {
        return callWithTracing(name, null, callable);
    }

    public static <T> T supplyWithTracing(@Nonnull String name, @Nonnull Supplier<T> supplier) {
        return supplyWithTracing(name, null, supplier);
    }

    public static void throwingRunWithTracing(@Nonnull String name, @Nonnull ThrowingRunnable throwingRunnable) throws Exception {
        throwingRunWithTracing(name, null, throwingRunnable);
    }

    private static ScopedSpan createScopedSpan(@Nonnull String name, @Nullable InternalMessage message) {
        return tracing.tracer().startScopedSpanWithParent(name, getTraceContext(message));
    }

    @Nullable
    public static ImmutableMap<String, String> getTraceData() {
        if (tracing != null) {
            Span currentSpan = tracing.tracer().currentSpan();
            if (currentSpan != null) {
                Builder<String, String> builder = ImmutableMap.builder();
                tracing.propagation().injector(SETTER).inject(currentSpan.context(), builder);
                return builder.build();
            }
        } else {
            logger.warn("Attempted to get trace data before initialization of Tracing bean");
        }
        return null;
    }

    private static TraceContext getTraceContext(@Nullable InternalMessage message) {
        if (message != null) {
            return tracing.propagation().extractor(GETTER).extract(message).context();
        }
        Span currentSpan = tracing.tracer().currentSpan();
        return currentSpan != null ? currentSpan.context() : null;
    }

}
