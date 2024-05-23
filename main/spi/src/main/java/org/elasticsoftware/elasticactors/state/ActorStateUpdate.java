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

package org.elasticsoftware.elasticactors.state;

import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.ElasticActor;
import org.elasticsoftware.elasticactors.tracing.CreationContext;
import org.elasticsoftware.elasticactors.tracing.TraceContext;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

/**
 * @author Joost van de Wijgerd
 */
public interface ActorStateUpdate {
    String getVersion();

    @Nullable
    ActorLifecycleStep getLifecycleStep();

    @Nullable
    Class getMessageClass();

    Class<? extends ElasticActor> getActorClass();

    ActorRef getActorRef();

    @Nullable
    ByteBuffer getSerializedState();

    @Nullable
    TraceContext getTraceContext();

    @Nullable
    CreationContext getCreationContext();
}
