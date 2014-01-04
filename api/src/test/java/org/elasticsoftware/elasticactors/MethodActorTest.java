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
    public void testMethodCall() throws Exception {
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
}
