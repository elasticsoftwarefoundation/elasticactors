package org.elasticsoftware.elasticactors.tracing.configuration;

import org.elasticsoftware.elasticactors.tracing.MessagingContextManager;
import org.elasticsoftware.elasticactors.tracing.TraceContext;

import javax.annotation.Nonnull;
import java.util.concurrent.Callable;

public class TraceCallable<V> implements Callable<V> {

    private final Callable<V> delegate;
    private final TraceContext parent;

    public static <V> TraceCallable<V> wrap(@Nonnull Callable<V> delegate) {
        if (delegate instanceof TraceCallable) {
            return (TraceCallable<V>) delegate;
        }
        return new TraceCallable<>(delegate);
    }

    private TraceCallable(Callable<V> delegate) {
        this.delegate = delegate;
        this.parent = MessagingContextManager.currentTraceContext();
    }

    @Override
    public V call() throws Exception {
        try (MessagingContextManager.MessagingScope ignored = MessagingContextManager.enter(new TraceContext(parent))) {
            return this.delegate.call();
        }
    }

}
