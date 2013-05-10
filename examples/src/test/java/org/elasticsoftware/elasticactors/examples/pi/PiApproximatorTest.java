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

package org.elasticsoftware.elasticactors.examples.pi;

import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.ListenableFuture;
import com.ning.http.client.Response;
import org.apache.log4j.BasicConfigurator;
import org.elasticsoftware.elasticactors.*;
import org.elasticsoftware.elasticactors.cluster.ActorRefFactory;
import org.elasticsoftware.elasticactors.examples.pi.actors.Master;
import org.elasticsoftware.elasticactors.examples.pi.messages.Calculate;
import org.elasticsoftware.elasticactors.examples.pi.messages.PiApproximation;
import org.elasticsoftware.elasticactors.examples.pi.messages.Result;
import org.elasticsoftware.elasticactors.examples.pi.messages.Work;
import org.elasticsoftware.elasticactors.serialization.Deserializer;
import org.elasticsoftware.elasticactors.serialization.MessageDeserializer;
import org.elasticsoftware.elasticactors.serialization.MessageSerializer;
import org.elasticsoftware.elasticactors.serialization.Serializer;
import org.elasticsoftware.elasticactors.test.TestActorSystem;
import org.elasticsoftware.elasticactors.http.HttpActorSystem;
import org.mockito.ArgumentCaptor;
import org.springframework.core.annotation.AnnotationUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.UUID;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

/**
 * @author Joost van de Wijgerd
 */
public class PiApproximatorTest {
    private TestActorSystem testActorSystem;

    @BeforeMethod
    public void setUp() {
        BasicConfigurator.resetConfiguration();
        BasicConfigurator.configure();
        testActorSystem = TestActorSystem.create();
    }

    @AfterMethod
    public void tearDown() {
        if(testActorSystem != null) {
            testActorSystem.destroy();
            testActorSystem = null;
        }
        BasicConfigurator.resetConfiguration();
    }

    @Test
    public void testMessageSerializers() throws IOException {
        PiApproximator piApproximator = new PiApproximator("PiTest",1);

        // Calculate

        MessageSerializer<Calculate> calculateMessageSerializer = piApproximator.getSerializer(Calculate.class);
        assertNotNull(calculateMessageSerializer);
        ByteBuffer serializedForm = calculateMessageSerializer.serialize(new Calculate());
        assertNotNull(serializedForm);
        MessageDeserializer<Calculate> calculateMessageDeserializer = piApproximator.getDeserializer(Calculate.class);
        assertNotNull(calculateMessageDeserializer);
        Calculate calculate = calculateMessageDeserializer.deserialize(serializedForm);
        assertNotNull(calculate);

        // Work
        MessageSerializer<Work> workMessageSerializer = piApproximator.getSerializer(Work.class);
        assertNotNull(workMessageSerializer);
        serializedForm = workMessageSerializer.serialize(new Work(1,100, "testId"));
        assertNotNull(serializedForm);
        MessageDeserializer<Work> workMessageDeserializer = piApproximator.getDeserializer(Work.class);
        assertNotNull(workMessageDeserializer);
        Work work = workMessageDeserializer.deserialize(serializedForm);
        assertNotNull(work);
        assertEquals(work.getStart(),1);
        assertEquals(work.getNrOfElements(),100);

        // REsult
        MessageSerializer<Result> resultMessageSerializer = piApproximator.getSerializer(Result.class);
        assertNotNull(workMessageSerializer);
        serializedForm = resultMessageSerializer.serialize(new Result(0.8376d, "testId"));
        assertNotNull(serializedForm);
        MessageDeserializer<Result> resultMessageDeserializer = piApproximator.getDeserializer(Result.class);
        assertNotNull(workMessageDeserializer);
        Result result = resultMessageDeserializer.deserialize(serializedForm);
        assertNotNull(result);
        assertEquals(result.getValue(),0.8376d);

        // PiApproximation
        MessageSerializer<PiApproximation> piApproximationMessageSerializer = piApproximator.getSerializer(PiApproximation.class);
        assertNotNull(piApproximationMessageSerializer);
        serializedForm = piApproximationMessageSerializer.serialize(new PiApproximation(UUID.randomUUID().toString(), 3.14827683d,19283827262l));
        MessageDeserializer<PiApproximation> piApproximationMessageDeserializer = piApproximator.getDeserializer(PiApproximation.class);
        assertNotNull(piApproximationMessageDeserializer);
        PiApproximation piApproximation = piApproximationMessageDeserializer.deserialize(serializedForm);
        assertNotNull(piApproximation);
        assertEquals(piApproximation.getPi(),3.14827683d);
        assertEquals(piApproximation.getDuration(),19283827262l);
    }

    @Test(enabled = false)
    public void testStateSerialization() throws Exception {
        ActorRef listenerRef = mock(ActorRef.class);
        ActorRef masterRef = mock(ActorRef.class);
        ActorSystem actorSystem = mock(ActorSystem.class);
        ActorSystems parent = mock(ActorSystems.class);
        ActorRefFactory actorRefFactory = mock(ActorRefFactory.class);
        PiApproximator piApproximator = new PiApproximator("PiTest",1);

        ArgumentCaptor<Master.State> stateArgumentCaptor = ArgumentCaptor.forClass(Master.State.class);

        when(actorSystem.getParent()).thenReturn(parent);
        when(parent.getActorRefFactory()).thenReturn(actorRefFactory);
        //when(actorSystem.actorOf(eq("pi/calculate"), eq(Listener.class), any(ActorState.class))).thenReturn(listenerRef);
        when(actorSystem.serviceActorFor("pi/calculate")).thenReturn(listenerRef);
        when(actorSystem.actorOf(eq("master"),eq(Master.class),stateArgumentCaptor.capture())).thenReturn(masterRef);


        when(listenerRef.toString()).thenReturn("listenerRef");
        when(masterRef.toString()).thenReturn("masterRef");
        when(actorRefFactory.create("listenerRef")).thenReturn(listenerRef);
        when(actorRefFactory.create("masterRef")).thenReturn(masterRef);

        piApproximator.initialize(actorSystem);
        piApproximator.create(actorSystem);

        Serializer<ActorState,byte[]> actorStateSerializer = piApproximator.getActorStateSerializer();
        assertNotNull(actorStateSerializer);

        byte[] serializedBytes = actorStateSerializer.serialize(piApproximator.getActorStateFactory().create(stateArgumentCaptor.getValue()));
        //System.out.println(new String(serializedBytes, Charsets.UTF_8));
        assertNotNull(serializedBytes);

        Deserializer<byte[],ActorState> actorStateDeserializer = piApproximator.getActorStateDeserializer();
        assertNotNull(actorStateDeserializer);

        ActorState actorState = actorStateDeserializer.deserialize(serializedBytes);
        assertNotNull(actorState);
        Master.State masterState = actorState.getAsObject(Master.State.class);
        assertNotNull(masterState);
        assertEquals(masterState.getListener(),listenerRef);
        assertEquals(masterState.getNrOfWorkers(),4);
        assertEquals(masterState.getNrOfMessages(),10000);
        assertEquals(masterState.getNrOfElements(),10000);

    }

    @Test
    public void testInContainer() throws Exception {
        // make sure http system is loaded
        testActorSystem.create(new HttpActorSystem());
        ActorSystem piSystem = testActorSystem.create(new PiApproximator("Pi",8));

        AsyncHttpClient httpClient = new AsyncHttpClient();
        ListenableFuture<Response> responseFuture = httpClient.prepareGet("http://localhost:8080/pi/calculate").execute();
        Response response = responseFuture.get();
        assertEquals(response.getContentType(),"application/json");
        System.out.println(response.getResponseBody("UTF-8"));
    }

    @Test
    public void testDependency() {
        assertNotNull(AnnotationUtils.findAnnotation(PiApproximator.class,DependsOn.class));
    }
}
