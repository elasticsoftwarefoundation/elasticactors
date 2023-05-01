/*
 * Copyright 2013 - 2023 The Original Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */

package org.elasticsoftware.elasticactors.test;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.ActorSystem;
import org.elasticsoftware.elasticactors.base.actors.ActorDelegate;
import org.elasticsoftware.elasticactors.base.actors.ReplyActor;
import org.elasticsoftware.elasticactors.base.serialization.JacksonSerializationFramework;
import org.elasticsoftware.elasticactors.cluster.InternalActorSystem;
import org.elasticsoftware.elasticactors.test.common.NameActor;
import org.elasticsoftware.elasticactors.test.common.NameActorState;
import org.elasticsoftware.elasticactors.test.messaging.LocalMessageQueue;
import org.elasticsoftwarefoundation.elasticactors.reflection.ApplyJsonPatch;
import org.elasticsoftwarefoundation.elasticactors.reflection.SerializedStateRequest;
import org.elasticsoftwarefoundation.elasticactors.reflection.SerializedStateResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class ReflectionTest {

    private static final Logger logger = LoggerFactory.getLogger(ReflectionTest.class);

    @Test
    public void testGetActorName() throws Exception {

        logger.info("Starting ReflectionTest");

        TestActorSystem testActorSystem = new TestActorSystem();
        testActorSystem.initialize();

        ActorSystem actorSystem = testActorSystem.getActorSystem();
        ActorRef actorRef =
            actorSystem.actorOf("test", NameActor.class, new NameActorState("Bob"));

        final CountDownLatch countDownLatch = new CountDownLatch(1);

        ActorRef replyActor = actorSystem.tempActorOf(ReplyActor.class, ActorDelegate.builder()
                .onReceive(
                        SerializedStateResponse.class,
                        m -> {
                            logger.info("Got current actor state '{}'", m.getState());

                            ByteBuffer serializedObject = ByteBuffer
                                .wrap(m.getState().getBytes(StandardCharsets.UTF_8));

                            NameActorState nameActorState =
                                (NameActorState) ((InternalActorSystem) actorSystem)
                                    .getParent()
                                    .getSerializationFramework(JacksonSerializationFramework.class)
                                    .getActorStateDeserializer(NameActor.class)
                                    .deserialize(serializedObject);

                            assertEquals(nameActorState.getName(), "Bob");
                        })
                .postReceive(countDownLatch::countDown)
                .build());

        actorRef.tell(SerializedStateRequest.INSTANCE, replyActor);

        assertTrue(countDownLatch.await(5, TimeUnit.SECONDS));
        assertTrue(LocalMessageQueue.getThrownExceptions().isEmpty());

        testActorSystem.destroy();
    }

    @Test
    public void testPatchState() throws Exception {

        logger.info("Starting ReflectionTest");

        TestActorSystem testActorSystem = new TestActorSystem();
        testActorSystem.initialize();

        ActorSystem actorSystem = testActorSystem.getActorSystem();
        ActorRef actorRef =
            actorSystem.actorOf("test", NameActor.class, new NameActorState("Bob"));



        ApplyJsonPatch message = getObjectMapper(testActorSystem).readValue("{\n"
            + "    \"patch\": [\n"
            + "        {\n"
            + "            \"op\": \"replace\",\n"
            + "            \"path\": \"/name\",\n"
            + "            \"value\": \"Alice\"\n"
            + "        }\n"
            + "    ]\n"
            + "}", ApplyJsonPatch.class);

        final CountDownLatch countDownLatch = new CountDownLatch(1);

        ActorRef replyActor = actorSystem.tempActorOf(ReplyActor.class, ActorDelegate.builder()
            .onReceive(
                SerializedStateResponse.class,
                m -> {
                    logger.info("Got current actor state '{}'", m.getState());

                    ByteBuffer serializedObject = ByteBuffer
                        .wrap(m.getState().getBytes(StandardCharsets.UTF_8));

                    NameActorState nameActorState =
                        (NameActorState) ((InternalActorSystem) actorSystem)
                            .getParent()
                            .getSerializationFramework(JacksonSerializationFramework.class)
                            .getActorStateDeserializer(NameActor.class)
                            .deserialize(serializedObject);

                    assertEquals(nameActorState.getName(), "Alice");
                })
            .postReceive(countDownLatch::countDown)
            .build());

        actorRef.tell(message, replyActor);

        assertTrue(countDownLatch.await(5, TimeUnit.SECONDS));
        assertTrue(LocalMessageQueue.getThrownExceptions().isEmpty());

        SerializedStateResponse stateResponse =
            actorRef.ask(SerializedStateRequest.INSTANCE, SerializedStateResponse.class)
                .get();

        NameActorState nameActorState = getObjectMapper(testActorSystem).readValue(
            stateResponse.getState(),
            NameActorState.class);
        assertEquals(nameActorState.getName(), "Alice");

        testActorSystem.destroy();
    }

    private ObjectMapper getObjectMapper(TestActorSystem testActorSystem) {
        JacksonSerializationFramework framework =
            (JacksonSerializationFramework) ((InternalActorSystem) testActorSystem.getActorSystem())
            .getParent()
            .getSerializationFramework(JacksonSerializationFramework.class);

        return framework.getObjectMapper();
    }

}
