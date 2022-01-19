/*
 *   Copyright 2013 - 2022 The Original Authors
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

package org.elasticsoftware.elasticactors.util.concurrent.metrics;

import org.elasticsoftware.elasticactors.tracing.CreationContext;
import org.elasticsoftware.elasticactors.tracing.MessagingContextManager.MessagingScope;
import org.elasticsoftware.elasticactors.tracing.TraceContext;
import org.elasticsoftware.elasticactors.util.concurrent.ThreadBoundRunnable;
import org.elasticsoftware.elasticactors.util.concurrent.WrapperThreadBoundRunnable;

import javax.annotation.Nonnull;

import static org.elasticsoftware.elasticactors.tracing.MessagingContextManager.getManager;

final class TraceThreadBoundRunnable<T> implements WrapperThreadBoundRunnable<T> {

    private final ThreadBoundRunnable<T> delegate;
    private final TraceContext parent;
    private final CreationContext creationContext;

    static <T> TraceThreadBoundRunnable<T> wrap(@Nonnull ThreadBoundRunnable<T> delegate) {
        if (delegate instanceof TraceThreadBoundRunnable) {
            return (TraceThreadBoundRunnable<T>) delegate;
        }
        return new TraceThreadBoundRunnable<>(delegate);
    }

    private TraceThreadBoundRunnable(@Nonnull ThreadBoundRunnable<T> delegate) {
        this.delegate = delegate;
        MessagingScope scope = getManager().currentScope();
        this.parent = scope != null ? scope.getTraceContext() : null;
        this.creationContext = scope != null ? scope.creationContextFromScope() : null;
    }

    @Override
    public void run() {
        try (MessagingScope ignored = getManager().enter(new TraceContext(parent), creationContext)) {
            this.delegate.run();
        }
    }

    @Override
    public T getKey() {
        return delegate.getKey();
    }

    @Nonnull
    @Override
    public ThreadBoundRunnable<T> getWrappedRunnable() {
        return delegate;
    }
}
