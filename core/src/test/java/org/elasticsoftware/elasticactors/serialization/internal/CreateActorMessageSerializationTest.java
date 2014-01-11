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

import org.elasticsoftware.elasticactors.cluster.InternalActorSystems;
import org.elasticsoftware.elasticactors.messaging.internal.ActorType;
import org.elasticsoftware.elasticactors.messaging.internal.CreateActorMessage;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

/**
 * @author Joost van de Wijgerd
 */
public class CreateActorMessageSerializationTest {
    @Test
    public void testSerialization() throws IOException {
        InternalActorSystems internalActorSystems = mock(InternalActorSystems.class);
        CreateActorMessageSerializer serializer = new CreateActorMessageSerializer(internalActorSystems);

        ByteBuffer serializedForm = serializer.serialize(new CreateActorMessage("LocalNode","testClass","listener",null));
        CreateActorMessageDeserializer deserializer = new CreateActorMessageDeserializer(internalActorSystems);

        CreateActorMessage deserializedMessage = deserializer.deserialize(serializedForm);
        assertNotNull(deserializedMessage);
        assertEquals(deserializedMessage.getActorClass(),"testClass");
        assertEquals(deserializedMessage.getActorSystem(),"LocalNode");
        assertEquals(deserializedMessage.getActorId(),"listener");
        assertEquals(deserializedMessage.getInitialState(),null);
        assertEquals(deserializedMessage.getType(), ActorType.PERSISTENT);

    }
}
