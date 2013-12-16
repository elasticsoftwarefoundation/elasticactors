/*
 * Copyright 2013 the original authors
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
import com.fasterxml.jackson.databind.type.MapType;
import com.fasterxml.jackson.databind.type.SimpleType;
import org.elasticsoftware.elasticactors.ActorState;
import org.elasticsoftware.elasticactors.ActorStateFactory;
import org.elasticsoftware.elasticactors.serialization.Deserializer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Joost van de Wijgerd
 */
public final class JacksonActorStateDeserializer implements Deserializer<byte[], ActorState> {
    private final ObjectMapper objectMapper;
    private final ActorStateFactory actorStateFactory;
    public static final MapType MAP_TYPE = MapType.construct(HashMap.class, SimpleType.construct(String.class), SimpleType.construct(Object.class));

    public JacksonActorStateDeserializer(ObjectMapper objectMapper, ActorStateFactory actorStateFactory) {
        this.objectMapper = objectMapper;
        this.actorStateFactory = actorStateFactory;
    }

    @Override
    public ActorState deserialize(byte[] serializedObject) throws IOException {
        Map<String,Object> jsonMap = objectMapper.readValue(serializedObject, MAP_TYPE);
        return actorStateFactory.create(jsonMap);
    }
}
