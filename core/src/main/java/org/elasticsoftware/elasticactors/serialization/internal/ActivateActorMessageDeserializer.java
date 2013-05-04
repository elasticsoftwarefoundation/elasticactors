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

package org.elasticsoftware.elasticactors.serialization.internal;

import com.google.protobuf.ByteString;
import org.elasticsoftware.elasticactors.messaging.internal.ActivateActorMessage;
import org.elasticsoftware.elasticactors.messaging.internal.ActorType;
import org.elasticsoftware.elasticactors.serialization.MessageDeserializer;
import org.elasticsoftware.elasticactors.serialization.protobuf.Elasticactors;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * @author Joost van de Wijgerd
 */
public final class ActivateActorMessageDeserializer implements MessageDeserializer<ActivateActorMessage> {


    public ActivateActorMessageDeserializer() {

    }

    @Override
    public ActivateActorMessage deserialize(ByteBuffer serializedObject) throws IOException {
        Elasticactors.ActivateActorMessage protobufMessage = Elasticactors.ActivateActorMessage.parseFrom(ByteString.copyFrom(serializedObject));
        return new ActivateActorMessage(protobufMessage.getActorSystem(),
                                        protobufMessage.getActorId(),
                                        protobufMessage.hasType()
                                              ? ActorType.values()[protobufMessage.getType().getNumber()]
                                              : ActorType.PERSISTENT);
    }


}
