package org.elasticsoftware.elasticactors.serialization.internal.tracing;

import org.elasticsoftware.elasticactors.serialization.protobuf.Messaging;
import org.elasticsoftware.elasticactors.tracing.CreationContext;

import javax.annotation.Nullable;

public final class CreationContextDeserializer {

    private CreationContextDeserializer() {
    }

    @Nullable
    public static CreationContext deserialize(Messaging.CreationContext creationContext) {
        if (hasAnyData(creationContext)) {
            CreationContext deserialized = new CreationContext(
                creationContext.hasCreator() ? creationContext.getCreator() : null,
                creationContext.hasCreatorType() ? creationContext.getCreatorType() : null,
                creationContext.hasCreatorMethod() ? creationContext.getCreatorMethod() : null,
                creationContext.hasScheduled() ? creationContext.getScheduled() : null
            );
            return deserialized.isEmpty() ? null : deserialized;
        } else {
            return null;
        }
    }

    private static boolean hasAnyData(Messaging.CreationContext creationContext) {
        return creationContext.hasCreator()
            || creationContext.hasCreatorType()
            || creationContext.hasCreatorMethod()
            || creationContext.hasScheduled();
    }

}
