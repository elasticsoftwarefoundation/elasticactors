/*
 * Copyright 2013 - 2023 The Original Authors
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

package org.elasticsoftware.elasticactors.tracing.spring;

import org.elasticsoftware.elasticactors.tracing.CreationContext;
import org.elasticsoftware.elasticactors.tracing.MessagingContextManager.MessagingScope;
import org.elasticsoftware.elasticactors.tracing.TraceContext;

import jakarta.annotation.Nonnull;

import static org.elasticsoftware.elasticactors.tracing.MessagingContextManager.getManager;

public class TraceRunnable implements Runnable {

    private final Runnable delegate;
    private final TraceContext parent;
    private final CreationContext creationContext;

    public static TraceRunnable wrap(@Nonnull Runnable delegate) {
        if (delegate instanceof TraceRunnable) {
            return (TraceRunnable) delegate;
        }
        return new TraceRunnable(delegate);
    }

    private TraceRunnable(@Nonnull Runnable delegate) {
        this.delegate = delegate;
        MessagingScope scope = getManager().currentScope();
        this.parent = scope != null ? scope.getTraceContext() : null;
        this.creationContext = scope != null ? scope.creationContextFromScope() : null;
    }

    @Override
    public void run() {
        try (MessagingScope ignored = getManager().enter(new TraceContext(parent), creationContext)) {
            this.delegate.run();
        }
    }

}
