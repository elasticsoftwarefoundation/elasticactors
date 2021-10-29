package org.elasticsoftware.elasticactors.tracing.configuration;

import org.elasticsoftware.elasticactors.tracing.CreationContext;
import org.elasticsoftware.elasticactors.tracing.MessagingContextManager.MessagingScope;
import org.elasticsoftware.elasticactors.tracing.TraceContext;

import javax.annotation.Nonnull;

import static org.elasticsoftware.elasticactors.tracing.MessagingContextManager.getManager;

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

}
