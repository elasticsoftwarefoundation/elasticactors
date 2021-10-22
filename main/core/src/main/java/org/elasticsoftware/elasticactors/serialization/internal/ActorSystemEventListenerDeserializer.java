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

package org.elasticsoftware.elasticactors.serialization.internal;

import org.elasticsoftware.elasticactors.cluster.ActorSystemEventListener;
import org.elasticsoftware.elasticactors.cluster.ActorSystemEventListenerImpl;
import org.elasticsoftware.elasticactors.serialization.Deserializer;
import org.elasticsoftware.elasticactors.serialization.protobuf.Elasticactors;

import java.io.IOException;

import static org.elasticsoftware.elasticactors.util.ClassLoadingHelper.getClassHelper;

/**
 * @author Joost van de Wijgerd
 */
public final class ActorSystemEventListenerDeserializer implements Deserializer<byte[],ActorSystemEventListener> {
    private static final ActorSystemEventListenerDeserializer INSTANCE = new ActorSystemEventListenerDeserializer();

    public static ActorSystemEventListenerDeserializer get() {
        return INSTANCE;
    }

    @Override
    public ActorSystemEventListener deserialize(byte[] serializedObject) throws IOException {
        try {
            Elasticactors.ActorSystemEventListener protobufMessage = Elasticactors.ActorSystemEventListener.parseFrom(serializedObject);
            Class messageClass = getClassHelper().forName(protobufMessage.getMessageClass());
            byte[] messageBytes = protobufMessage.getMessage().toByteArray();
            String actorId = protobufMessage.getActorId();
            return new ActorSystemEventListenerImpl(actorId, messageClass, messageBytes);
        } catch(ClassNotFoundException e) {
            throw new IOException(e);
        }
    }
}
