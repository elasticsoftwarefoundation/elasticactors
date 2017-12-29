/*
 * Copyright 2013 - 2017 The Original Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.elasticsoftware.elasticactors.state;

import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.ElasticActor;
import org.elasticsoftware.elasticactors.util.concurrent.ThreadBoundEvent;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

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

    public ActorStateUpdateEvent(Class<? extends ElasticActor> actorClass,
                                 ActorRef actorRef, ByteBuffer serializedState,
                                 String version, ActorLifecycleStep lifecycleStep,
                                 Class messageClass) {
        this.actorClass = actorClass;
        this.actorRef = actorRef;
        this.serializedState = serializedState;
        this.version = version;
        this.lifecycleStep = lifecycleStep;
        this.messageClass = messageClass;
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
        return serializedState;
    }

    public ActorStateUpdateEvent copyOf() {
        return new ActorStateUpdateEvent(actorClass, actorRef, serializedState.duplicate(), version, lifecycleStep, messageClass);
    }
}
