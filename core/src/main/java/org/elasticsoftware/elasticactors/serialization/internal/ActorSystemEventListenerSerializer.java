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

package org.elasticsoftware.elasticactors.serialization.internal;

import com.google.protobuf.ByteString;
import org.elasticsoftware.elasticactors.cluster.ActorSystemEventListener;
import org.elasticsoftware.elasticactors.serialization.Serializer;
import org.elasticsoftware.elasticactors.serialization.protobuf.Elasticactors;

/**
 * @author Joost van de Wijgerd
 */
public final class ActorSystemEventListenerSerializer implements Serializer<ActorSystemEventListener,byte[]> {
    private static final ActorSystemEventListenerSerializer INSTANCE = new ActorSystemEventListenerSerializer();

    public static ActorSystemEventListenerSerializer get() {
        return INSTANCE;
    }

    @Override
    public byte[] serialize(ActorSystemEventListener eventListener) {
        Elasticactors.ActorSystemEventListener.Builder builder = Elasticactors.ActorSystemEventListener.newBuilder();
        builder.setActorId(eventListener.getActorId());
        builder.setMessageClass(eventListener.getMessageClass().getName());
        builder.setMessage(ByteString.copyFrom(eventListener.getMessageBytes()));
        return builder.build().toByteArray();
    }

}
