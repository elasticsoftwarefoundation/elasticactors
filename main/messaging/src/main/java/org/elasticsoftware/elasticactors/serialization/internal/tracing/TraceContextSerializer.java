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
import org.elasticsoftware.elasticactors.tracing.TraceContext;

public final class TraceContextSerializer {

    private TraceContextSerializer() {
    }

    @Nullable
    public static Messaging.TraceContext serialize(@Nullable TraceContext traceContext) {
        if (traceContext != null && !traceContext.isEmpty()) {
            Messaging.TraceContext.Builder serialized = Messaging.TraceContext.newBuilder();
            serialized.setSpanId(traceContext.getSpanId());
            serialized.setTraceId(traceContext.getTraceId());
            if (traceContext.getParentId() != null) {
                serialized.setParentId(traceContext.getParentId());
            }
            if (traceContext.getBaggage() != null && !traceContext.getBaggage().isEmpty()) {
                serialized.putAllBaggage(traceContext.getBaggage());
            }
            return serialized.build();
        }
        return null;
    }

}
