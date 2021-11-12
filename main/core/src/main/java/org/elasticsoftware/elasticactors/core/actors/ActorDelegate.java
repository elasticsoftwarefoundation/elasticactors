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
import org.elasticsoftware.elasticactors.serialization.NoopSerializationFramework;
import org.elasticsoftware.elasticactors.serialization.SerializationFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

/**
 * @author Joost van de Wijgerd
 */
public abstract class ActorDelegate<T> extends TypedActor<T> implements ActorState<ActorDelegate<T>> {

    private static final Logger staticLogger = LoggerFactory.getLogger(ActorDelegate.class);

    /**
     * Default implementation that uses the static logger for {@link ActorDelegate}.
     * Although the user can override it, {@link ActorDelegate}  is often used for anonymous
     * subclassing, so this is a valuable optimization.
     */
    @Override
    protected Logger initLogger() {
        return staticLogger;
    }

    private final boolean deleteAfterReceive;

    private final ActorRef callerRef;

    protected ActorDelegate() {
        this(true);
    }

    protected ActorDelegate(boolean deleteAfterReceive) {
        this(deleteAfterReceive, null);
    }

    protected ActorDelegate(boolean deleteAfterReceive, ActorRef callerRef) {
        this.deleteAfterReceive = deleteAfterReceive;
        this.callerRef = callerRef;
    }

    public boolean isDeleteAfterReceive() {
        return deleteAfterReceive;
    }

    @Nullable
    public ActorRef getCallerRef() {
        return callerRef;
    }

    @Override
    public ActorDelegate<T> getBody() {
        return this;
    }

    @Override
    public Class<? extends SerializationFramework> getSerializationFramework() {
        return NoopSerializationFramework.class;
    }
}
