package org.elasticsoftware.elasticactors.tracing;

import org.elasticsoftware.elasticactors.ActorRef;

import javax.annotation.Nullable;

public interface TracedMessage extends Traceable {

    @Nullable
    ActorRef getSender();

    String getTypeAsString();

    @Nullable
    Class<?> getType();
}
