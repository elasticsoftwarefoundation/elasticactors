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
import brave.propagation.TraceContext;
import org.elasticsoftware.elasticactors.messaging.InternalMessage;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public final class TraceHelper {

    private static final Getter<InternalMessage, String> GETTER =
            (c, k) -> c.getTraceData() != null ? c.getTraceData().get(k) : null;

    public static void run(@Nonnull Runnable r, @Nullable InternalMessage m, @Nonnull String name) {
        Tracing tracing = Tracing.current();
        if (tracing != null) {
            TraceContext parent = getTraceContext(tracing, m);
            ScopedSpan span = tracing.tracer().startScopedSpanWithParent(name, parent);
            try {
                r.run();
            } catch (Throwable t) {
                span.error(t);
                throw t;
            } finally {
                span.finish();
            }
        } else {
            r.run();
        }
    }

    public static void onError(Throwable t) {
        Tracing tracing = Tracing.current();
        if (tracing != null) {
            Span current = tracing.tracer().currentSpan();
            if (current != null) {
                current.error(t);
            }
        }
    }

    private static TraceContext getTraceContext(Tracing tracing, @Nullable InternalMessage message) {
        if (message != null) {
            return tracing.propagation().extractor(GETTER).extract(message).context();
        }
        Span currentSpan = tracing.tracer().currentSpan();
        return currentSpan != null ? currentSpan.context() : null;
    }

}
