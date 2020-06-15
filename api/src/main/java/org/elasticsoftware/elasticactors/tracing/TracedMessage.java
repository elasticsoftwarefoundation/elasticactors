package org.elasticsoftware.elasticactors.tracing;

import org.elasticsoftware.elasticactors.ActorRef;

import javax.annotation.Nullable;

public interface TracedMessage {

    @Nullable
    ActorRef getSender();

    String getType();

    @Nullable
    TraceContext getTraceContext();

    @Nullable
    CreationContext getCreationContext();

}
