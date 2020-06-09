package org.elasticsoftware.elasticactors.tracing.configuration;

import org.elasticsoftware.elasticactors.tracing.MessagingContextManager;
import org.elasticsoftware.elasticactors.tracing.TraceContext;

import javax.annotation.Nonnull;

public class TraceRunnable implements Runnable {

    private final Runnable delegate;
    private final TraceContext parent;

    public static TraceRunnable wrap(@Nonnull Runnable delegate) {
        if (delegate instanceof TraceRunnable) {
            return (TraceRunnable) delegate;
        }
        return new TraceRunnable(delegate);
    }

    private TraceRunnable(@Nonnull Runnable delegate) {
        this.delegate = delegate;
        this.parent = MessagingContextManager.currentTraceContext();
    }

    @Override
    public void run() {
        try (MessagingContextManager.MessagingScope ignored = MessagingContextManager.enter(new TraceContext(parent))) {
            this.delegate.run();
        }
    }

}
