package org.elasticsoftware.elasticactors.tracing.configuration;

import org.elasticsoftware.elasticactors.tracing.CreationContext;
import org.elasticsoftware.elasticactors.tracing.MessagingContextManager.MessagingScope;
import org.elasticsoftware.elasticactors.tracing.TraceContext;

import javax.annotation.Nonnull;
import java.util.concurrent.Callable;

import static org.elasticsoftware.elasticactors.tracing.MessagingContextManager.getManager;

public class TraceCallable<V> implements Callable<V> {

    private final Callable<V> delegate;
    private final TraceContext parent;
    private final CreationContext creationContext;

    public static <V> TraceCallable<V> wrap(@Nonnull Callable<V> delegate) {
        if (delegate instanceof TraceCallable) {
            return (TraceCallable<V>) delegate;
        }
        return new TraceCallable<>(delegate);
    }

    private TraceCallable(Callable<V> delegate) {
        this.delegate = delegate;
        this.parent = getManager().currentTraceContext();
        this.creationContext = getManager().creationContextFromScope();
    }

    @Override
    public V call() throws Exception {
        try (MessagingScope ignored = getManager().enter(new TraceContext(parent), creationContext)) {
            return this.delegate.call();
        }
    }

}
