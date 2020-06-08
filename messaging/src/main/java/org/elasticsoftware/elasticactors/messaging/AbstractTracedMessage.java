package org.elasticsoftware.elasticactors.messaging;

import org.elasticsoftware.elasticactors.tracing.CreationContext;
import org.elasticsoftware.elasticactors.tracing.MessageHandlingContext;
import org.elasticsoftware.elasticactors.tracing.MessagingContextManager;
import org.elasticsoftware.elasticactors.tracing.TraceContext;
import org.elasticsoftware.elasticactors.tracing.TracedMessage;

import javax.annotation.Nullable;

import static org.elasticsoftware.elasticactors.tracing.MessagingContextManager.currentCreationContext;
import static org.elasticsoftware.elasticactors.tracing.MessagingContextManager.currentMessageHandlingContext;
import static org.elasticsoftware.elasticactors.tracing.MessagingContextManager.currentTraceContext;

public abstract class AbstractTracedMessage implements TracedMessage {

    private final TraceContext traceContext;
    private final CreationContext creationContext;

    protected AbstractTracedMessage() {
        TraceContext traceContext = currentTraceContext();
        this.traceContext = traceContext != null ? traceContext : new TraceContext();
        MessageHandlingContext messageHandlingContext = currentMessageHandlingContext();
        if (messageHandlingContext != null) {
            this.creationContext = new CreationContext(
                    messageHandlingContext.getReceiver(),
                    messageHandlingContext.getReceiverType(),
                    MessagingContextManager.currentMethodContext());
        } else {
            this.creationContext = currentCreationContext();
        }
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
