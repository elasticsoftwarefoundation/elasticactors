package org.elasticsoftware.elasticactors.util.concurrent;

import org.elasticsoftware.elasticactors.tracing.CreationContext;
import org.elasticsoftware.elasticactors.tracing.MessagingContextManager.MessagingScope;
import org.elasticsoftware.elasticactors.tracing.TraceContext;

import javax.annotation.Nonnull;

import static org.elasticsoftware.elasticactors.tracing.MessagingContextManager.getManager;

public class TraceThreadBoundRunnable<T> implements ThreadBoundRunnable<T> {

    private final ThreadBoundRunnable<T> delegate;
    private final TraceContext parent;
    private final CreationContext creationContext;

    public static <T> TraceThreadBoundRunnable<T> wrap(@Nonnull ThreadBoundRunnable<T> delegate) {
        if (delegate instanceof TraceThreadBoundRunnable) {
            return (TraceThreadBoundRunnable<T>) delegate;
        }
        return new TraceThreadBoundRunnable<>(delegate);
    }

    private TraceThreadBoundRunnable(@Nonnull ThreadBoundRunnable<T> delegate) {
        this.delegate = delegate;
        this.parent = getManager().currentTraceContext();
        this.creationContext = getManager().creationContextFromScope();
    }

    @Override
    public void run() {
        try (MessagingScope ignored = getManager().enter(
                new TraceContext(parent),
                creationContext)) {
            this.delegate.run();
        }
    }

    @Override
    public T getKey() {
        return delegate.getKey();
    }
}
