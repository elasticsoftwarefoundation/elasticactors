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
                creationContext.getCreator(),
                creationContext.getCreatorType(),
                creationContext.getCreatorMethod());
        if (creationContext.hasScheduled() && creationContext.getScheduled()) {
            deserialized = CreationContext.forScheduling(deserialized);
        }
        return deserialized.isEmpty() ? null : deserialized;
    }

}
