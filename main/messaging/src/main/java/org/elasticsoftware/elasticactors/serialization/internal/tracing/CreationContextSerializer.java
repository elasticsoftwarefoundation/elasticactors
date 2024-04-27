/*
 * Copyright 2013 - 2024 The Original Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */

package org.elasticsoftware.elasticactors.serialization.internal.tracing;

import jakarta.annotation.Nullable;
import org.elasticsoftware.elasticactors.serialization.protobuf.Messaging;
import org.elasticsoftware.elasticactors.tracing.CreationContext;

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
