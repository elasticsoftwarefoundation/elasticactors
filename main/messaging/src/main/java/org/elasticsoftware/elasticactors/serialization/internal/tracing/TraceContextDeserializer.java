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

import org.elasticsoftware.elasticactors.serialization.protobuf.Messaging;
import org.elasticsoftware.elasticactors.tracing.TraceContext;

import javax.annotation.Nullable;
import java.util.Map;

public final class TraceContextDeserializer {

    private TraceContextDeserializer() {
    }

    @Nullable
    public static TraceContext deserialize(Messaging.TraceContext traceContext) {
        if (hasMinimumAmountOfData(traceContext)) {
            Map<String, String> baggage = traceContext.getBaggageMap();
            TraceContext deserialized = new TraceContext(
                traceContext.hasSpanId() ? traceContext.getSpanId() : "",
                traceContext.hasTraceId() ? traceContext.getTraceId() : "",
                traceContext.hasParentId() ? traceContext.getParentId() : null,
                baggage.isEmpty() ? null : baggage,
                true
            );
            return deserialized.isEmpty() ? null : deserialized;
        } else {
            return null;
        }
    }

    private static boolean hasMinimumAmountOfData(Messaging.TraceContext traceContext) {
        return traceContext.hasSpanId() && traceContext.hasTraceId();
    }

}
