/*
 * Copyright 2013 - 2023 The Original Authors
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

import org.elasticsoftware.elasticactors.cluster.ActorRefFactory;
import org.elasticsoftware.elasticactors.serialization.MessageDeserializer;
import org.elasticsoftware.elasticactors.serialization.MessageSerializer;
import org.elasticsoftware.elasticactors.serialization.SerializationFramework;
import org.elasticsoftware.elasticactors.serialization.SerializationFrameworks;
import org.elasticsoftware.elasticactors.serialization.SystemDeserializers;
import org.elasticsoftware.elasticactors.serialization.SystemSerializers;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public final class ClientSerializationFrameworks implements SerializationFrameworks {

    private final SystemSerializers systemSerializers;
    private final SystemDeserializers systemDeserializers;
    private final Map<Class<? extends SerializationFramework>, SerializationFramework> serializationFrameworkMap;

    public ClientSerializationFrameworks(
            ActorRefFactory actorRefFactory,
            List<SerializationFramework> serializationFrameworks) {
        this.systemSerializers = new ClientSystemSerializers(this);
        this.systemDeserializers = new ClientSystemDeserializers(this, actorRefFactory);
        this.serializationFrameworkMap = serializationFrameworks.stream()
                .collect(Collectors.toMap(SerializationFramework::getClass, Function.identity()));
    }

    @Override
    public <T> MessageSerializer<T> getSystemMessageSerializer(Class<T> messageClass) {
        return systemSerializers.get(messageClass);
    }

    @Override
    public <T> MessageDeserializer<T> getSystemMessageDeserializer(Class<T> messageClass) {
        return systemDeserializers.get(messageClass);
    }

    @Override
    public SerializationFramework getSerializationFramework(
            Class<? extends SerializationFramework> frameworkClass) {
        return serializationFrameworkMap.get(frameworkClass);
    }
}
