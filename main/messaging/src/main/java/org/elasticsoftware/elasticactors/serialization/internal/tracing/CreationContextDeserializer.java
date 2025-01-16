/*
 * Copyright 2013 - 2025 The Original Authors
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
