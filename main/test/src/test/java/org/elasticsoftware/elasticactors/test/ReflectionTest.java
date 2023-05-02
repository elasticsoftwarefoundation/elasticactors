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
import org.elasticsoftware.elasticactors.base.serialization.JacksonSerializationFramework;
import org.elasticsoftware.elasticactors.cluster.InternalActorSystem;
import org.elasticsoftware.elasticactors.test.common.NameActor;
import org.elasticsoftware.elasticactors.test.common.NameActorState;
import org.elasticsoftwarefoundation.elasticactors.reflection.ApplyJsonPatch;
import org.elasticsoftwarefoundation.elasticactors.reflection.SerializedStateRequest;
import org.elasticsoftwarefoundation.elasticactors.reflection.SerializedStateResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class ReflectionTest {

    private static final Logger logger = LoggerFactory.getLogger(ReflectionTest.class);

    private TestActorSystem testActorSystem;

    @BeforeMethod
    public void setUp() {
        testActorSystem = new TestActorSystem();
        testActorSystem.initialize();
    }

    @AfterMethod
    public void tearDown() {
        testActorSystem.destroy();
    }

    @Test
    public void shouldGetActorState() throws Exception {
        // Given
        ActorSystem actorSystem = testActorSystem.getActorSystem();
        ActorRef actorRef =
            actorSystem.actorOf("test", NameActor.class, new NameActorState("Bob"));

        // When
        SerializedStateResponse response =
            actorRef.ask(SerializedStateRequest.INSTANCE, SerializedStateResponse.class)
                .get();

        // Then
        logger.info("Received state: {}", response.getState());

        NameActorState nameActorState =
            getObjectMapper(testActorSystem).readValue(response.getState(), NameActorState.class);

        assertEquals(nameActorState.getName(), "Bob");
    }

    @Test
    public void shouldPatchState() throws Exception {
        // Given
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
            + "    ],\n"
            + "    \"dryRun\": false\n"
            + "}", ApplyJsonPatch.class);

        // When
        SerializedStateResponse patchResponse =
            actorRef.ask(message, SerializedStateResponse.class).get();

        // Then
        logger.info("Received patched state: {}", patchResponse.getState());

        NameActorState patchedState = getObjectMapper(testActorSystem)
            .readValue(patchResponse.getState(), NameActorState.class);
        assertEquals(patchedState.getName(), "Alice");

        SerializedStateResponse stateResponse =
            actorRef.ask(SerializedStateRequest.INSTANCE, SerializedStateResponse.class)
                .get();

        // Double check that state is indeed changed
        NameActorState nameActorState = getObjectMapper(testActorSystem).readValue(
            stateResponse.getState(),
            NameActorState.class);
        Assert.assertEquals(nameActorState.getName(), "Alice");
    }

    @Test
    public void shouldDryRunPatchState() throws Exception {
        // Given
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
            + "    ],\n"
            + "    \"dryRun\": true\n"
            + "}", ApplyJsonPatch.class);

        // When
        SerializedStateResponse patchResponse =
            actorRef.ask(message, SerializedStateResponse.class).get();

        // Then
        logger.info("Received patched state: {}", patchResponse.getState());

        NameActorState patchedState = getObjectMapper(testActorSystem)
            .readValue(patchResponse.getState(), NameActorState.class);
        assertEquals(patchedState.getName(), "Alice");

        SerializedStateResponse stateResponse =
            actorRef.ask(SerializedStateRequest.INSTANCE, SerializedStateResponse.class)
                .get();

        // Double check that state is indeed changed
        NameActorState nameActorState = getObjectMapper(testActorSystem).readValue(
            stateResponse.getState(),
            NameActorState.class);
        Assert.assertEquals(nameActorState.getName(), "Bob");
    }

    private ObjectMapper getObjectMapper(TestActorSystem testActorSystem) {
        JacksonSerializationFramework framework =
            (JacksonSerializationFramework) ((InternalActorSystem) testActorSystem.getActorSystem())
            .getParent()
            .getSerializationFramework(JacksonSerializationFramework.class);

        return framework.getObjectMapper();
    }

}
