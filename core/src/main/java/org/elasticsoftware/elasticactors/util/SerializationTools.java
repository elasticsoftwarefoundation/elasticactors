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

package org.elasticsoftware.elasticactors.util;

import org.elasticsoftware.elasticactors.cluster.InternalActorSystem;
import org.elasticsoftware.elasticactors.messaging.InternalMessage;
import org.elasticsoftware.elasticactors.serialization.MessageDeserializer;

/**
 * @author Joost van de Wijgerd
 */
public final class SerializationTools {
    public static Object deserializeMessage(InternalActorSystem actorSystem,InternalMessage internalMessage) throws Exception {
        MessageDeserializer deserializer = actorSystem.getDeserializer(Class.forName(internalMessage.getPayloadClass()));
        if(deserializer != null) {
            return internalMessage.getPayload(deserializer);
        } else {
            //@todo: throw a more targeted exception
            throw new Exception(String.format("No Deserializer found for Message class %s in ActorSystem [%s]",
                                                    internalMessage.getPayloadClass(),actorSystem.getName()));
        }
    }
}
