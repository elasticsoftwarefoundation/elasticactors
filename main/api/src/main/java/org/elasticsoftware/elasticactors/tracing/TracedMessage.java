package org.elasticsoftware.elasticactors.tracing;

import org.elasticsoftware.elasticactors.ActorRef;

import javax.annotation.Nullable;

public interface TracedMessage {

    @Nullable
    ActorRef getSender();

    String getTypeAsString();

    @Nullable
    Class<?> getType();

    @Nullable
    TraceContext getTraceContext();

    @Nullable
    CreationContext getCreationContext();

}
