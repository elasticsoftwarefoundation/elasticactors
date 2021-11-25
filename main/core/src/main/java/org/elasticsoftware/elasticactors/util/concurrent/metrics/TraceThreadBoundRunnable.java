package org.elasticsoftware.elasticactors.util.concurrent.metrics;

import org.elasticsoftware.elasticactors.tracing.CreationContext;
import org.elasticsoftware.elasticactors.tracing.MessagingContextManager.MessagingScope;
import org.elasticsoftware.elasticactors.tracing.TraceContext;
import org.elasticsoftware.elasticactors.util.concurrent.ThreadBoundRunnable;

import javax.annotation.Nonnull;

import static org.elasticsoftware.elasticactors.tracing.MessagingContextManager.getManager;

final class TraceThreadBoundRunnable<T> implements ThreadBoundRunnable<T> {

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
}
