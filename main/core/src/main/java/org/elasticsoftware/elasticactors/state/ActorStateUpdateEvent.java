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

package org.elasticsoftware.elasticactors.state;

import jakarta.annotation.Nullable;
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.ElasticActor;
import org.elasticsoftware.elasticactors.tracing.CreationContext;
import org.elasticsoftware.elasticactors.tracing.MessagingContextManager.MessagingScope;
import org.elasticsoftware.elasticactors.tracing.TraceContext;
import org.elasticsoftware.elasticactors.util.concurrent.ThreadBoundEvent;

import java.nio.ByteBuffer;

import static org.elasticsoftware.elasticactors.tracing.MessagingContextManager.getManager;

/**
 * @author Joost van de Wijgerd
 */
public final class ActorStateUpdateEvent implements ThreadBoundEvent<String>, ActorStateUpdate {
    private final Class<? extends ElasticActor> actorClass;
    private final ActorRef actorRef;
    private final ByteBuffer serializedState;
    private final String version;
    private final ActorLifecycleStep lifecycleStep;
    private final Class messageClass;
    private final TraceContext traceContext;
    private final CreationContext creationContext;

    public ActorStateUpdateEvent(
            Class<? extends ElasticActor> actorClass,
            ActorRef actorRef,
            ByteBuffer serializedState,
            String version,
            ActorLifecycleStep lifecycleStep,
            Class messageClass) {
        this(
            actorClass,
            actorRef,
            serializedState,
            version,
            lifecycleStep,
            messageClass,
            getManager().currentScope()
        );
    }

    private ActorStateUpdateEvent(Class<? extends ElasticActor> actorClass,
        ActorRef actorRef,
        ByteBuffer serializedState,
        String version,
        ActorLifecycleStep lifecycleStep,
        Class messageClass,
        MessagingScope scope)
    {
        this(
            actorClass,
            actorRef,
            serializedState,
            version,
            lifecycleStep,
            messageClass,
            scope != null ? scope.getTraceContext() : null,
            scope != null ? scope.creationContextFromScope() : null
        );
    }

    private ActorStateUpdateEvent(
            Class<? extends ElasticActor> actorClass,
            ActorRef actorRef,
            ByteBuffer serializedState,
            String version,
            ActorLifecycleStep lifecycleStep,
            Class messageClass,
            TraceContext traceContext,
            CreationContext creationContext) {
        this.actorClass = actorClass;
        this.actorRef = actorRef;
        this.serializedState = serializedState;
        this.version = version;
        this.lifecycleStep = lifecycleStep;
        this.messageClass = messageClass;
        this.traceContext = traceContext;
        this.creationContext = creationContext;
    }


    @Override
    public String getKey() {
        return actorRef.getActorId();
    }

    @Override
    public String getVersion() {
        return version;
    }

    @Nullable
    @Override
    public ActorLifecycleStep getLifecycleStep() {
        return lifecycleStep;
    }

    @Nullable
    @Override
    public Class getMessageClass() {
        return messageClass;
    }

    @Override
    public Class<? extends ElasticActor> getActorClass() {
        return actorClass;
    }

    @Override
    public ActorRef getActorRef() {
        return actorRef;
    }

    @Nullable
    @Override
    public ByteBuffer getSerializedState() {
        // Using duplicate to give implementations a chance to access the internal byte array
        // Duplicate byte buffer, so it can be safely reused
        return serializedState != null ? serializedState.duplicate() : null;
    }

    @Nullable
    @Override
    public TraceContext getTraceContext() {
        return traceContext;
    }

    @Nullable
    @Override
    public CreationContext getCreationContext() {
        return creationContext;
    }
}
