/*
 * Copyright 2013 - 2016 The Original Authors
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

package org.elasticsoftware.elasticactors.base.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsoftware.elasticactors.ActorState;
import org.elasticsoftware.elasticactors.serialization.Serializer;

import java.io.IOException;

/**
 * @author Joost van de Wijgerd
 */
public final class JacksonActorStateSerializer implements Serializer<ActorState, byte[]> {
    private final ObjectMapper objectMapper;

    public JacksonActorStateSerializer(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public byte[] serialize(ActorState object) throws IOException {
        return objectMapper.writeValueAsBytes(object);
    }
}
