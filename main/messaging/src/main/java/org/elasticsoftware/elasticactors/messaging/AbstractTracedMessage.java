package org.elasticsoftware.elasticactors.messaging;

import org.elasticsoftware.elasticactors.tracing.CreationContext;
import org.elasticsoftware.elasticactors.tracing.TraceContext;
import org.elasticsoftware.elasticactors.tracing.TracedMessage;

import javax.annotation.Nullable;

import static org.elasticsoftware.elasticactors.tracing.MessagingContextManager.getManager;

public abstract class AbstractTracedMessage implements TracedMessage {

    private final TraceContext traceContext;
    private final CreationContext creationContext;

    protected AbstractTracedMessage() {
        TraceContext traceContext = getManager().currentTraceContext();
        this.traceContext = traceContext != null ? traceContext : new TraceContext();
        this.creationContext = getManager().creationContextFromScope();
    }

    protected AbstractTracedMessage(
            TraceContext traceContext,
            CreationContext creationContext) {
        this.traceContext = traceContext;
        this.creationContext = creationContext;
    }

    @Nullable
    @Override
    public final TraceContext getTraceContext() {
        return traceContext;
    }

    @Nullable
    @Override
    public final CreationContext getCreationContext() {
        return creationContext;
    }

}
