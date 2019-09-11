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

import brave.Span;
import brave.Tracer.SpanInScope;
import brave.Tracing;
import brave.propagation.Propagation.Getter;
import brave.propagation.Propagation.Setter;
import brave.propagation.TraceContextOrSamplingFlags;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import org.elasticsoftware.elasticactors.ActorContextHolder;
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.ActorSystem;
import org.elasticsoftware.elasticactors.messaging.InternalMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Configurable;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.function.Supplier;

@Configurable
public final class TracerImpl implements Tracer {

    private static final Getter<InternalMessage, String> GETTER =
            (c, k) -> c.getTraceData() != null ? c.getTraceData().get(k) : null;
    private static final Setter<Builder<String, String>, String> SETTER = Builder::put;

    private static final Logger logger = LoggerFactory.getLogger(Tracer.class);

    @Autowired
    private Tracing tracing;

    @Override
    public void runWithTracing(
            @Nonnull String name,
            @Nullable InternalMessage message,
            @Nonnull Runnable runnable) {
        Span span = createSpan(name, message);
        try (SpanInScope ignore = tracing.tracer().withSpanInScope(span)) {
            tagWithActorContextData(span);
            runnable.run();
        } catch (Throwable t) {
            span.error(t);
            throw t;
        } finally {
            span.finish();
        }
    }

    @Override
    public <E extends Throwable> void throwingRunWithTracing(
            @Nonnull String name,
            @Nullable InternalMessage message,
            @Nonnull ThrowingRunnable<E> throwingRunnable) throws E {
        Span span = createSpan(name, message);
        tagWithActorContextData(span);
        try (SpanInScope ignore = tracing.tracer().withSpanInScope(span)) {
            throwingRunnable.run();
        } catch (Throwable t) {
            span.error(t);
            throw t;
        } finally {
            span.finish();
        }
    }

    @Override
    public <T> T supplyWithTracing(
            @Nonnull String name,
            @Nullable InternalMessage message,
            @Nonnull Supplier<T> supplier) {
        Span span = createSpan(name, message);
        tagWithActorContextData(span);
        try (SpanInScope ignore = tracing.tracer().withSpanInScope(span)) {
            return supplier.get();
        } catch (Throwable t) {
            span.error(t);
            throw t;
        } finally {
            span.finish();
        }
    }

    @Override
    public <T, E extends Throwable> T throwingSupplyWithTracing(
            @Nonnull String name,
            @Nullable InternalMessage message,
            @Nonnull ThrowingSupplier<T, E> throwingSupplier) throws E {
        Span span = createSpan(name, message);
        tagWithActorContextData(span);
        try (SpanInScope ignore = tracing.tracer().withSpanInScope(span)) {
            return throwingSupplier.get();
        } catch (Throwable t) {
            span.error(t);
            throw t;
        } finally {
            span.finish();
        }
    }

    @Override
    public void runInCurrentTrace(@Nonnull Runnable runnable) {
        Span currentSpan = tracing.tracer().currentSpan();
        if (currentSpan != null) {
            try {
                runnable.run();
            } catch (Throwable t) {
                currentSpan.error(t);
                throw t;
            }
        } else {
            logger.warn("Attempted to execute Runnable with tracing, but there was no trace!");
            runnable.run();
        }
    }

    @Override
    public <E extends Throwable> void throwingRunInCurrentTrace(
            @Nonnull ThrowingRunnable<E> throwingRunnable) throws E {
        Span currentSpan = tracing.tracer().currentSpan();
        if (currentSpan != null) {
            try {
                throwingRunnable.run();
            } catch (Throwable t) {
                currentSpan.error(t);
                throw t;
            }
        } else {
            logger.warn(
                    "Attempted to execute ThrowingRunnable with tracing, but there was no trace!");
            throwingRunnable.run();
        }
    }

    @Override
    public <T> T supplyInCurrentTrace(@Nonnull Supplier<T> supplier) {
        Span currentSpan = tracing.tracer().currentSpan();
        if (currentSpan != null) {
            try {
                return supplier.get();
            } catch (Throwable t) {
                currentSpan.error(t);
                throw t;
            }
        } else {
            logger.warn("Attempted to execute Supplier with tracing, but there was no trace!");
            return supplier.get();
        }
    }

    @Override
    public <T, E extends Throwable> T throwingSupplyInCurrentTrace(
            @Nonnull ThrowingSupplier<T, E> throwingSupplier) throws E {
        Span currentSpan = tracing.tracer().currentSpan();
        if (currentSpan != null) {
            try {
                return throwingSupplier.get();
            } catch (Throwable t) {
                currentSpan.error(t);
                throw t;
            }
        } else {
            logger.warn(
                    "Attempted to execute ThrowingSupplier with tracing, but there was no trace!");
            return throwingSupplier.get();
        }
    }

    @Nonnull
    private Span createSpan(
            @Nonnull String name,
            @Nullable InternalMessage message) {
        if (message != null) {
            TraceContextOrSamplingFlags extracted =
                    tracing.propagation().extractor(GETTER).extract(message);
            if (extracted.context() != null) {
                return tracing.tracer().nextSpan(extracted).name(name);
            }
        }
        return tracing.tracer().nextSpan().name(name);
    }

    @Override
    @Nullable
    public ImmutableMap<String, String> getTraceData() {
        Span currentSpan = tracing.tracer().currentSpan();
        if (currentSpan != null) {
            Builder<String, String> builder = ImmutableMap.builder();
            tracing.propagation().injector(SETTER).inject(currentSpan.context(), builder);
            return builder.build();
        }
        return null;
    }

    private static void tagWithActorContextData(Span span) {
        if (ActorContextHolder.hasActorContext()) {
            ActorRef self = ActorContextHolder.getSelf();
            ActorSystem system = ActorContextHolder.getSystem();
            span.tag("actor.id", String.valueOf(self != null ? self.getActorId() : null));
            span.tag("actorsystem.name", system.getName());
            span.tag("actorsystem.version", system.getConfiguration().getVersion());
        }
    }

}
