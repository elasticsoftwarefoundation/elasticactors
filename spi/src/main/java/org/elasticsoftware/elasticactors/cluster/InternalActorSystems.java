/*
 * Copyright 2013 - 2014 The Original Authors
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

package org.elasticsoftware.elasticactors.cluster;

import org.elasticsoftware.elasticactors.ActorSystems;
import org.elasticsoftware.elasticactors.serialization.MessageDeserializer;
import org.elasticsoftware.elasticactors.serialization.MessageSerializer;
import org.elasticsoftware.elasticactors.serialization.SerializationFramework;

/**
 * @author Joost van de Wijgerd
 */
public interface InternalActorSystems extends ActorSystems {
    InternalActorSystem get(String name);

    <T> MessageSerializer<T> getSystemMessageSerializer(Class<T> messageClass);

    <T> MessageDeserializer<T> getSystemMessageDeserializer(Class<T> messageClass);

    SerializationFramework getSerializationFramework(Class<? extends SerializationFramework> frameworkClass);
}
