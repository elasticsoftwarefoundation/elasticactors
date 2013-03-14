/*
 * Copyright (c) 2013 Joost van de Wijgerd <jwijgerd@gmail.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.elasterix.elasticactors.examples.pi;

import org.codehaus.jackson.map.ObjectMapper;
import org.elasterix.elasticactors.ActorState;
import org.elasterix.elasticactors.ActorSystemConfiguration;
import org.elasterix.elasticactors.examples.common.JacksonActorStateDeserializer;
import org.elasterix.elasticactors.examples.common.JacksonActorStateSerializer;
import org.elasterix.elasticactors.serialization.Deserializer;
import org.elasterix.elasticactors.serialization.MessageDeserializer;
import org.elasterix.elasticactors.serialization.MessageSerializer;
import org.elasterix.elasticactors.serialization.Serializer;

import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class PiApproximator implements ActorSystemConfiguration {
    private final String name;
    private final int numberOfShards;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Serializer<ActorState, byte[]> actorStateSerializer = new JacksonActorStateSerializer(objectMapper);
    private final Deserializer<byte[],ActorState> actorStateDeserializer = new JacksonActorStateDeserializer(objectMapper);
    private final Map<Class<?>,MessageSerializer<?>> messageSerializers = new HashMap<Class<?>,MessageSerializer<?>>() {{

    }};
    private final Map<Class<?>,MessageDeserializer<?>> messageDeserializers = new HashMap<Class<?>,MessageDeserializer<?>>() {{

    }};

    public PiApproximator(String name, int numberOfShards) {
        this.name = name;
        this.numberOfShards = numberOfShards;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public int getNumberOfShards() {
        return numberOfShards;
    }

    @Override
    public String getVersion() {
        return "0.1.0";
    }

    @Override
    public <T> MessageSerializer<T> getSerializer(Class<T> messageClass) {
        return (MessageSerializer<T>) messageSerializers.get(messageClass);
    }

    @Override
    public <T> MessageDeserializer<T> getDeserializer(Class<T> messageClass) {
        return (MessageDeserializer<T>) messageDeserializers.get(messageClass);
    }

    @Override
    public Serializer<ActorState, byte[]> getActorStateSerializer() {
        return actorStateSerializer;
    }

    @Override
    public Deserializer<byte[], ActorState> getActorStateDeserializer() {
        return actorStateDeserializer;
    }
}
