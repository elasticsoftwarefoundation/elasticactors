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

package org.elasticsoftware.elasticactors.tracing;

import org.elasticsoftware.elasticactors.tracing.MessagingContextManager.MessagingScope;

import jakarta.annotation.Nullable;
import java.lang.reflect.Method;

public final class NoopMessagingScope implements MessagingScope {

    public final static NoopMessagingScope INSTANCE = new NoopMessagingScope();

    private NoopMessagingScope() {
    }

    @Nullable
    @Override
    public TraceContext getTraceContext() {
        return null;
    }

    @Nullable
    @Override
    public CreationContext getCreationContext() {
        return null;
    }

    @Nullable
    @Override
    public CreationContext creationContextFromScope() {
        return null;
    }

    @Nullable
    @Override
    public MessageHandlingContext getMessageHandlingContext() {
        return null;
    }

    @Nullable
    @Override
    public Method getMethod() {
        return null;
    }

    @Override
    public boolean isClosed() {
        return false;
    }

    @Override
    public void close() {

    }

    @Override
    public String toString() {
        return NoopMessagingScope.class.getSimpleName() + "{}";
    }
}