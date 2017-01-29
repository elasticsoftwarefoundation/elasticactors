/*
 * Copyright 2013 - 2017 The Original Authors
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

package org.elasticsoftware.elasticactors.reactivestreams;

import org.elasticsoftware.elasticactors.Actor;
import org.elasticsoftware.elasticactors.ActorSystem;
import org.elasticsoftware.elasticactors.MessageHandler;
import org.elasticsoftware.elasticactors.base.serialization.JacksonSerializationFramework;

import java.util.concurrent.TimeUnit;

/**
 * @author Joost van de Wijgerd
 */
@Actor(stateClass = TestPublishingActorState.class, serializationFramework = JacksonSerializationFramework.class)
public final class TestPublishingActor extends PublishingActor {


    @Override
    public void postActivate(String previousVersion) throws Exception {
        getSystem().getScheduler().scheduleOnce(getSelf(), new GenerateTestMessages(1), getSelf(), 1, TimeUnit.SECONDS);
    }

    @MessageHandler
    public void handle(GenerateTestMessages message, ActorSystem actorSystem) {
        publish(message);
        getSystem().getScheduler().scheduleOnce(getSelf(), new GenerateTestMessages(message.getTotal()+1), getSelf(), 1, TimeUnit.SECONDS);
    }
}
