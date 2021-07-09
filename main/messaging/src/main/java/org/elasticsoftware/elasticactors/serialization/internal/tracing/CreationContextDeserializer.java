package org.elasticsoftware.elasticactors.serialization.internal.tracing;

import org.elasticsoftware.elasticactors.serialization.protobuf.Messaging;
import org.elasticsoftware.elasticactors.tracing.CreationContext;

import javax.annotation.Nullable;

public final class CreationContextDeserializer {

    private CreationContextDeserializer() {
    }

    @Nullable
    public static CreationContext deserialize(Messaging.CreationContext creationContext) {
        CreationContext deserialized = new CreationContext(
                creationContext.hasCreator() ? creationContext.getCreator() : null,
                creationContext.hasCreatorType() ? creationContext.getCreatorType() : null,
                creationContext.hasCreatorMethod() ? creationContext.getCreatorMethod() : null,
                creationContext.hasScheduled() ? creationContext.getScheduled() : null);
        return deserialized.isEmpty() ? null : deserialized;
    }

}
