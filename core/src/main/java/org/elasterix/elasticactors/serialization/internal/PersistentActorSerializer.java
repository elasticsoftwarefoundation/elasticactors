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

import com.google.protobuf.ByteString;
import org.elasterix.elasticactors.messaging.InternalMessage;
import org.elasterix.elasticactors.messaging.UUIDTools;
import org.elasterix.elasticactors.serialization.Serializer;
import org.elasterix.elasticactors.serialization.protobuf.Elasticactors;
import org.elasterix.elasticactors.state.PersistentActor;

/**
 * @author Joost van de Wijgerd
 */
public class PersistentActorSerializer implements Serializer<PersistentActor,byte[]> {
    private static final PersistentActorSerializer INSTANCE = new PersistentActorSerializer();

    public static PersistentActorSerializer get() {
        return INSTANCE;
    }

    @Override
    public byte[] serialize(PersistentActor persistentActor) {
        Elasticactors.PersistentActor.Builder builder = Elasticactors.PersistentActor.newBuilder();
        builder.setActorSystemVersion(persistentActor.getActorSystemVersion());
        builder.setActorClass(persistentActor.getActorClass());
        builder.setActorRef(persistentActor.getRef());
        if (persistentActor.getSerializedState() != null) {
            builder.setState(ByteString.copyFrom(persistentActor.getSerializedState()));
        }
        return builder.build().toByteArray();
    }


}
