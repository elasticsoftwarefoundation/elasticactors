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

package org.elasticsoftware.elasticactors.base.serialization;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.afterburner.AfterburnerModule;
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.cluster.ActorRefFactory;
import org.elasticsoftware.elasticactors.cluster.scheduler.ScheduledMessageRefFactory;
import org.elasticsoftware.elasticactors.scheduler.ScheduledMessageRef;
import org.testng.annotations.Test;

import java.math.BigDecimal;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

/**
 * @author Joost van de Wijgerd
 */
public class ObjectMapperBuilderTest {
    @Test
    public void testBuildObjectMapper() throws JsonProcessingException {
        ActorRefFactory actorRefFactory = mock(ActorRefFactory.class);
        ScheduledMessageRefFactory scheduledMessageRefFactory = mock(ScheduledMessageRefFactory.class);
        ActorRef ref = mock(ActorRef.class);
        ScheduledMessageRef scheduledMessageRef = mock(ScheduledMessageRef.class);
        when(ref.toString()).thenReturn("actor://test");
        when(scheduledMessageRef.toString()).thenReturn("message://test");
        when(actorRefFactory.create(anyString())).thenReturn(ref);
        when(scheduledMessageRefFactory.create(anyString())).thenReturn(scheduledMessageRef);

        ObjectMapper objectMapper = new ObjectMapperBuilder(actorRefFactory,scheduledMessageRefFactory,"1.0.0").build();
        // check if everything was scanned correctly
        assertNotNull(objectMapper);

        TestSerDeser toSerialize = new TestSerDeser(ref, scheduledMessageRef);
        String serialized = objectMapper.writeValueAsString(toSerialize);
        assertEquals(
            serialized,
            "{\"ref\":\"actor://test\",\"scheduledMessageRef\":\"message://test\"}"
        );

        TestSerDeser deserialized = objectMapper.readValue(serialized, TestSerDeser.class);
        assertNotNull(deserialized);
        assertNotNull(deserialized.getRef());
        assertNotNull(deserialized.getScheduledMessageRef());
        assertEquals(deserialized.getRef(), toSerialize.getRef());
        assertEquals(deserialized.getScheduledMessageRef(), toSerialize.getScheduledMessageRef());
    }

    @JsonInclude(NON_NULL)
    @JsonPropertyOrder({"ref", "scheduledMessageRef"})
    public final static class TestSerDeser {
        private final ActorRef ref;
        private final ScheduledMessageRef scheduledMessageRef;

        @JsonCreator
        public TestSerDeser(
            @JsonProperty("ref") ActorRef ref,
            @JsonProperty("scheduledRef") ScheduledMessageRef scheduledMessageRef)
        {
            this.ref = ref;
            this.scheduledMessageRef = scheduledMessageRef;
        }

        public ActorRef getRef() {
            return ref;
        }

        public ScheduledMessageRef getScheduledMessageRef() {
            return scheduledMessageRef;
        }
    }

    // wait for release 2.4.5
    @Test(enabled = false)
    public void testAfterburnerModule() throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new AfterburnerModule());

        objectMapper.writeValueAsString(new TestObjectWIthJsonSerialize(new BigDecimal("870.04")));
    }
}
