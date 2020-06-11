package org.elasticsoftware.elasticactors.serialization.internal.tracing;

import org.elasticsoftware.elasticactors.serialization.protobuf.Messaging;
import org.elasticsoftware.elasticactors.tracing.CreationContext;

import javax.annotation.Nullable;

public final class CreationContextSerializer {

    private CreationContextSerializer() {
    }

    @Nullable
    public static Messaging.CreationContext serialize(@Nullable CreationContext creationContext) {
        if (creationContext != null && !creationContext.isEmpty()) {
            Messaging.CreationContext.Builder serialized = Messaging.CreationContext.newBuilder();
            if (creationContext.getCreator() != null) {
                serialized.setCreator(creationContext.getCreator());
            }
            if (creationContext.getCreatorType() != null) {
                serialized.setCreatorType(creationContext.getCreatorType());
            }
            if (creationContext.getCreatorMethod() != null) {
                serialized.setCreatorMethod(creationContext.getCreatorMethod());
            }
            if (creationContext.getScheduled() != null) {
                serialized.setScheduled(creationContext.getScheduled());
            }
            return serialized.build();
        }
        return null;
    }

}
