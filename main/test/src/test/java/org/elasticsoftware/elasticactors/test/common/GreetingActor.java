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

package org.elasticsoftware.elasticactors.test.common;

import org.elasticsoftware.elasticactors.Actor;
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.TypedActor;
import org.elasticsoftware.elasticactors.base.serialization.JacksonSerializationFramework;
import org.elasticsoftware.elasticactors.base.state.StringState;
import org.elasticsoftware.elasticactors.scheduler.ScheduledMessageRef;
import org.elasticsoftware.elasticactors.tracing.CreationContext;
import org.elasticsoftware.elasticactors.tracing.MessagingContextManager.MessagingScope;
import org.elasticsoftware.elasticactors.tracing.TraceContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

import static org.elasticsoftware.elasticactors.test.common.GreetingTest.TEST_TRACE;
import static org.elasticsoftware.elasticactors.tracing.MessagingContextManager.getManager;
import static org.elasticsoftware.elasticactors.tracing.TracingUtils.shorten;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

/**
 * @author Joost van de Wijgerd
 */
@Actor(stateClass = StringState.class,serializationFramework = JacksonSerializationFramework.class)
public final class GreetingActor extends TypedActor<Greeting> {

    private static final Logger staticLogger = LoggerFactory.getLogger(GreetingActor.class);

    @Override
    protected Logger initLogger() {
        return staticLogger;
    }

    @Override
    public void onReceive(ActorRef sender, Greeting message) throws Exception {
        logger.info("Hello, {}", message.getWho());
        if (getManager().isTracingEnabled()) {
            MessagingScope scope = getManager().currentScope();
            assertNotNull(scope);
            assertNull(scope.getMethod());
            CreationContext creationContext = scope.getCreationContext();
            assertNotNull(creationContext);
            assertNull(creationContext.getScheduled());
            assertEquals(creationContext.getCreator(), GreetingTest.class.getSimpleName());
            assertEquals(creationContext.getCreatorType(), shorten(GreetingTest.class.getName()));
            TraceContext current = scope.getTraceContext();
            assertNotNull(current);
            assertNotEquals(current.getSpanId(), TEST_TRACE.getSpanId());
            assertEquals(current.getTraceId(), TEST_TRACE.getTraceId());
            assertEquals(current.getParentId(), TEST_TRACE.getSpanId());
        }
        ScheduledMessageRef messageRef = getSystem().getScheduler()
            .scheduleOnce(new Greeting("Greeting Actor"), sender, 1, TimeUnit.SECONDS);
        sender.tell(new ScheduledGreeting(messageRef));
    }
}
