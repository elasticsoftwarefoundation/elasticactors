/*
 *   Copyright 2013 - 2019 The Original Authors
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.elasticsoftware.elasticactors.base.actors;

import org.elasticsoftware.elasticactors.ActorContext;
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.TestActorContextHolder;
import org.elasticsoftware.elasticactors.base.state.JavaScriptActorState;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Joost van de Wijgerd
 */
public class JavaScriptActorTest {
    @Test
    public void testCompile() throws Exception {
        ActorContext context = mock(ActorContext.class);
        ActorRef sender = mock(ActorRef.class);

        when(sender.getActorId()).thenReturn("testactor");

        TestActorContextHolder.setContext(context);
        JavaScriptActorState state = new JavaScriptActorState("var onReceive = function(sender, message) {\n" +
                "    print(\"JS Class Definition: \" + Object.prototype.toString.call(sender));\n" +
                "    print('Got message from sender, ' + sender.actorId);\n" +
                "    return \"BLA\";\n" +
                "};");

        when(context.getState(JavaScriptActorState.class)).thenReturn(state);

        JavaScriptActor actor = new JavaScriptActor();

        actor.postActivate(null);

        actor.onReceive(sender, new Object());
        actor.onReceive(sender, new Object());
        actor.onReceive(sender, new Object());

    }
}
