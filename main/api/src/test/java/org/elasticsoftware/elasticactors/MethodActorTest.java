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

package org.elasticsoftware.elasticactors;

import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

/**
 * @author Joost van de Wijgerd
 */
public class MethodActorTest {
    @Test
    public void testMethodCallOnImpl() throws Exception {
        ActorContext context = mock(ActorContext.class);
        ActorRef sender = mock(ActorRef.class);
        ActorRef self = mock(ActorRef.class);
        ActorSystem actorSystem = mock(ActorSystem.class);
        TestActorContextHolder.setContext(context);
        TestActorState state = new TestActorState();
        when(context.getState(TestActorState.class)).thenReturn(state);
        when(context.getActorSystem()).thenReturn(actorSystem);

        TestPersistentMethodActor actor = new TestPersistentMethodActor();
        actor.onReceive(sender,new TestMessage("hello world!"));

        assertNotNull(state.getActorSystem());
        assertEquals(state.getActorSystem(),actorSystem);

        assertNotNull(state.getSender());
    }

    @Test
    public void testMethodCallOnExternalClass() throws Exception {
        ActorContext context = mock(ActorContext.class);
        ActorRef sender = mock(ActorRef.class);
        ActorRef self = mock(ActorRef.class);
        ActorSystem actorSystem = mock(ActorSystem.class);
        TestActorContextHolder.setContext(context);
        TestActorState state = new TestActorState();
        when(context.getState(TestActorState.class)).thenReturn(state);
        when(context.getActorSystem()).thenReturn(actorSystem);

        TestPersistentMethodActor actor = new TestPersistentMethodActor();
        actor.onReceive(sender,new AnotherTestMessage("hello world!"));

        assertNotNull(state.getActorSystem());
        assertEquals(state.getActorSystem(),actorSystem);

        assertNotNull(state.getSender());
    }
}
