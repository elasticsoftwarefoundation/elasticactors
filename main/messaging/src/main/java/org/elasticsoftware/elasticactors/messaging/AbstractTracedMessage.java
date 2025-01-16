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

package org.elasticsoftware.elasticactors.messaging;

import org.elasticsoftware.elasticactors.tracing.CreationContext;
import org.elasticsoftware.elasticactors.tracing.MessagingContextManager.MessagingScope;
import org.elasticsoftware.elasticactors.tracing.TraceContext;
import org.elasticsoftware.elasticactors.tracing.TracedMessage;

import jakarta.annotation.Nullable;

import static org.elasticsoftware.elasticactors.tracing.MessagingContextManager.getManager;

public abstract class AbstractTracedMessage implements TracedMessage {

    private final TraceContext traceContext;
    private final CreationContext creationContext;

    protected AbstractTracedMessage() {
        MessagingScope scope = getManager().currentScope();
        TraceContext traceContext = scope != null ? scope.getTraceContext() : null;
        this.traceContext = traceContext != null ? traceContext : new TraceContext();
        this.creationContext = scope != null ? scope.creationContextFromScope() : null;
    }

    protected AbstractTracedMessage(
            TraceContext traceContext,
            CreationContext creationContext) {
        this.traceContext = traceContext;
        this.creationContext = creationContext;
    }

    @Nullable
    @Override
    public final TraceContext getTraceContext() {
        return traceContext;
    }

    @Nullable
    @Override
    public final CreationContext getCreationContext() {
        return creationContext;
    }

}
