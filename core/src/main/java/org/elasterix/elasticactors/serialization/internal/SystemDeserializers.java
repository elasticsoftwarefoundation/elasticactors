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
import org.elasterix.elasticactors.ActorSystems;
import org.elasterix.elasticactors.cluster.InternalActorSystems;
import org.elasterix.elasticactors.messaging.internal.CreateActorMessage;
import org.elasterix.elasticactors.serialization.MessageDeserializer;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Joost van de Wijgerd
 */
public final class SystemDeserializers {
    private final InternalActorSystems cluster;
    private final Map<Class,MessageDeserializer> systemDeserializers = new HashMap<Class,MessageDeserializer>();

    public SystemDeserializers(InternalActorSystems cluster) {
        this.cluster = cluster;
        systemDeserializers.put(CreateActorMessage.class,new CreateActorMessageDeserializer(cluster));
        systemDeserializers.put(String.class,new HectorMessageDeserializer<String>(StringSerializer.get()));
        //@todo: add more deserializers here
    }

    public <T> MessageDeserializer<T> get(Class<T> messageClass) {
        return systemDeserializers.get(messageClass);
    }
}
