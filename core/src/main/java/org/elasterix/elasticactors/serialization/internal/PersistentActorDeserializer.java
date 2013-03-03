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

import org.elasterix.elasticactors.ActorRef;
import org.elasterix.elasticactors.messaging.InternalMessage;
import org.elasterix.elasticactors.messaging.InternalMessageImpl;
import org.elasterix.elasticactors.messaging.UUIDTools;
import org.elasterix.elasticactors.serialization.Deserializer;
import org.elasterix.elasticactors.serialization.protobuf.Elasticactors;
import org.elasterix.elasticactors.state.PersistentActor;

import java.io.IOException;
import java.util.UUID;

/**
 * @author Joost van de Wijgerd
 */
public class PersistentActorDeserializer implements Deserializer<byte[],PersistentActor> {
    private static final PersistentActorDeserializer INSTANCE = new PersistentActorDeserializer();

    public static PersistentActorDeserializer get() {
        return INSTANCE;
    }

    @Override
    public PersistentActor deserialize(byte[] serializedObject) throws IOException {
        Elasticactors.PersistentActor protobufMessage = Elasticactors.PersistentActor.parseFrom(serializedObject);
        byte[] serializedState = protobufMessage.hasState() ? protobufMessage.getState().toByteArray(): null;
        return new PersistentActor(protobufMessage.getActorSystemVersion(),protobufMessage.getActorRef(),
                                   protobufMessage.getActorClass(),serializedState);
    }
}
