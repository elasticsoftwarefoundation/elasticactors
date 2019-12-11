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


package org.elasticsoftware.elasticactors.client.state;

import org.elasticsoftware.elasticactors.ActorState;
import org.elasticsoftware.elasticactors.client.serialization.SerializedActorStateSerializationFramework;
import org.elasticsoftware.elasticactors.serialization.SerializationFramework;

/**
 * This abstract class should allow for thinner clients that do not need to create a new
 * implementation of {@link ActorState} for every new actor type they need to create, providing a
 * properly-constructed serialized representation of the intended actor state.
 */
public abstract class SerializedActorState implements ActorState<byte[]> {

    @Override
    public Class<? extends SerializationFramework> getSerializationFramework() {
        return SerializedActorStateSerializationFramework.class;
    }
}
