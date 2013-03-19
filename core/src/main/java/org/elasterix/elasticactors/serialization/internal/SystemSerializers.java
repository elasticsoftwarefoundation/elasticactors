/*
 * Copyright 2013 Joost van de Wijgerd
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

package org.elasterix.elasticactors.serialization.internal;

import me.prettyprint.cassandra.serializers.StringSerializer;
import org.elasterix.elasticactors.cluster.InternalActorSystems;
import org.elasterix.elasticactors.messaging.internal.CreateActorMessage;
import org.elasterix.elasticactors.serialization.MessageSerializer;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Joost van de Wijgerd
 */
public final class SystemSerializers {
    private final Map<Class,MessageSerializer> systemSerializers = new HashMap<Class,MessageSerializer>();

    public SystemSerializers(InternalActorSystems cluster) {
        systemSerializers.put(CreateActorMessage.class,new CreateActorMessageSerializer(cluster));
        systemSerializers.put(String.class,new HectorMessageSerializer<String>(StringSerializer.get()));
                //@todo: add more serializers here
    }

    public <T> MessageSerializer<T> get(Class<T> messageClass) {
        return systemSerializers.get(messageClass);
    }
}
