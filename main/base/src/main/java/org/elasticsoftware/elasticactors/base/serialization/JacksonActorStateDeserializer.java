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

package org.elasticsoftware.elasticactors.base.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.util.ByteBufferBackedInputStream;
import org.elasticsoftware.elasticactors.ActorState;
import org.elasticsoftware.elasticactors.base.state.JacksonActorState;
import org.elasticsoftware.elasticactors.serialization.Deserializer;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * @author Joost van de Wijgerd
 */
public final class JacksonActorStateDeserializer implements Deserializer<ByteBuffer, ActorState> {
    private final ObjectMapper objectMapper;

    public JacksonActorStateDeserializer(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public ActorState deserialize(ByteBuffer serializedObject) throws IOException {
        if (serializedObject.hasArray()) {
            return objectMapper.readValue(serializedObject.array(), JacksonActorState.class);
        } else {
            // Avoid marking the buffer (what if it was marked from the outside?)
            int position = serializedObject.position();
            try {
                return objectMapper.readValue(
                    new ByteBufferBackedInputStream(serializedObject),
                    JacksonActorState.class
                );
            } finally {
                serializedObject.position(position);
            }
        }
    }

    @Override
    public boolean isSafe() {
        return true;
    }
}
