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


package org.elasticsoftware.elasticactors.client.serialization;

import org.elasticsoftware.elasticactors.ActorState;
import org.elasticsoftware.elasticactors.client.state.SerializedActorState;
import org.elasticsoftware.elasticactors.serialization.Serializer;

import java.io.IOException;

/**
 * A serializer for {@link SerializedActorState} that simply returns its internal byte array
 */
public final class SerializedActorStateSerializer implements Serializer<ActorState, byte[]> {

    @Override
    public byte[] serialize(ActorState object) throws IOException {
        if (object instanceof SerializedActorState) {
            return ((SerializedActorState) object).getBody();
        }
        throw new IllegalArgumentException("Only ActorSystemState is supported by this serializer");
    }
}
