/*
 *   Copyright 2013 - 2019 The Original Authors
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.elasticsoftware.elasticactors.core.actors;

import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.ActorState;
import org.elasticsoftware.elasticactors.TypedActor;
import org.elasticsoftware.elasticactors.concurrent.Expirable;
import org.elasticsoftware.elasticactors.serialization.NoopSerializationFramework;
import org.elasticsoftware.elasticactors.serialization.SerializationFramework;
import org.elasticsoftware.elasticactors.tracing.CreationContext;
import org.elasticsoftware.elasticactors.tracing.MessagingContextManager;
import org.elasticsoftware.elasticactors.tracing.MessagingContextManager.MessagingScope;
import org.elasticsoftware.elasticactors.tracing.TraceContext;
import org.elasticsoftware.elasticactors.tracing.Traceable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * @author Joost van de Wijgerd
 */
public abstract class InternalActorDelegate<T>
    extends TypedActor<T>
    implements ActorState<InternalActorDelegate<T>>, Traceable, Expirable {

    private static final Logger staticLogger = LoggerFactory.getLogger(InternalActorDelegate.class);

    /**
     * Default implementation that uses the static logger for {@link InternalActorDelegate}.
     */
    @Override
    protected Logger initLogger() {
        return staticLogger;
    }

    private final boolean deleteAfterReceive;
    private final ActorRef callerRef;
    private final TraceContext traceContext;
    private final CreationContext creationContext;
    private final long expirationTime;

    protected InternalActorDelegate() {
        this(true);
    }

    protected InternalActorDelegate(boolean deleteAfterReceive) {
        this(deleteAfterReceive, null);
    }

    protected InternalActorDelegate(boolean deleteAfterReceive, long timeoutMillis) {
        this(deleteAfterReceive, null, timeoutMillis);
    }

    protected InternalActorDelegate(
        boolean deleteAfterReceive,
        long timeout,
        @Nonnull TimeUnit timeUnit)
    {
        this(deleteAfterReceive, null, timeout, timeUnit);
    }

    protected InternalActorDelegate(boolean deleteAfterReceive, ActorRef callerRef) {
        this(deleteAfterReceive, callerRef, TEMP_ACTOR_TIMEOUT_DEFAULT);
    }

    protected InternalActorDelegate(
        boolean deleteAfterReceive,
        ActorRef callerRef,
        long timeoutMillis)
    {
        this.deleteAfterReceive = deleteAfterReceive;
        this.callerRef = callerRef;
        MessagingScope currentScope = MessagingContextManager.getManager().currentScope();
        if (currentScope != null) {
            traceContext = currentScope.getTraceContext();
            creationContext = currentScope.getCreationContext();
        } else {
            traceContext = null;
            creationContext = null;
        }
        this.expirationTime = System.currentTimeMillis() + Expirable.clamp(
            timeoutMillis,
            TEMP_ACTOR_TIMEOUT_MIN,
            TEMP_ACTOR_TIMEOUT_MAX
        );
    }

    protected InternalActorDelegate(
        boolean deleteAfterReceive,
        ActorRef callerRef,
        long timeout,
        @Nonnull TimeUnit timeUnit)
    {
        this(deleteAfterReceive, callerRef, Objects.requireNonNull(timeUnit).toMillis(timeout));
    }

    public boolean isDeleteAfterReceive() {
        return deleteAfterReceive;
    }

    @Nullable
    public ActorRef getCallerRef() {
        return callerRef;
    }

    @Override
    @Nullable
    public TraceContext getTraceContext() {
        return traceContext;
    }

    @Override
    @Nullable
    public CreationContext getCreationContext() {
        return creationContext;
    }

    @Override
    public InternalActorDelegate<T> getBody() {
        return this;
    }

    @Override
    public Class<? extends SerializationFramework> getSerializationFramework() {
        return NoopSerializationFramework.class;
    }

    @Override
    public final long getExpirationTime() {
        return expirationTime;
    }
}
