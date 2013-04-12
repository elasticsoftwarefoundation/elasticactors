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
import org.elasterix.elasticactors.ActorState;
import org.elasterix.elasticactors.cluster.InternalActorSystem;
import org.elasterix.elasticactors.cluster.InternalActorSystems;
import org.elasterix.elasticactors.messaging.internal.CreateActorMessage;
import org.elasterix.elasticactors.messaging.internal.DestroyActorMessage;
import org.elasterix.elasticactors.serialization.MessageSerializer;
import org.elasterix.elasticactors.serialization.protobuf.Elasticactors;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * @author Joost van de Wijgerd
 */
public final class DestroyActorMessageSerializer implements MessageSerializer<DestroyActorMessage> {


    public DestroyActorMessageSerializer() {
    }


    @Override
    public ByteBuffer serialize(DestroyActorMessage message) throws IOException {
        Elasticactors.DestroyActorMessage.Builder builder = Elasticactors.DestroyActorMessage.newBuilder();
        builder.setActorRef(message.getActorRef().toString());
        return ByteBuffer.wrap(builder.build().toByteArray());
    }

}
