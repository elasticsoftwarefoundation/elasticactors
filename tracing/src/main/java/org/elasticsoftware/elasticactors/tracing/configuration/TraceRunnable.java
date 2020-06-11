package org.elasticsoftware.elasticactors.tracing.configuration;

import org.elasticsoftware.elasticactors.tracing.CreationContext;
import org.elasticsoftware.elasticactors.tracing.MessagingContextManager.MessagingScope;
import org.elasticsoftware.elasticactors.tracing.TraceContext;

import javax.annotation.Nonnull;

import static org.elasticsoftware.elasticactors.tracing.MessagingContextManager.creationContextFromScope;
import static org.elasticsoftware.elasticactors.tracing.MessagingContextManager.currentTraceContext;
import static org.elasticsoftware.elasticactors.tracing.MessagingContextManager.enter;

public class TraceRunnable implements Runnable {

    private final Runnable delegate;
    private final TraceContext parent;
    private final CreationContext creationContext;

    public static TraceRunnable wrap(@Nonnull Runnable delegate) {
        if (delegate instanceof TraceRunnable) {
            return (TraceRunnable) delegate;
        }
        return new TraceRunnable(delegate);
    }

    private TraceRunnable(@Nonnull Runnable delegate) {
        this.delegate = delegate;
        this.parent = currentTraceContext();
        this.creationContext = creationContextFromScope();
    }

    @Override
    public void run() {
        try (MessagingScope ignored = enter(new TraceContext(parent), creationContext)) {
            this.delegate.run();
        }
    }

}
