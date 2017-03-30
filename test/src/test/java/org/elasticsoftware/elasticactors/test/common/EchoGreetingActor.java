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

package org.elasticsoftware.elasticactors.test.common;

import org.elasticsoftware.elasticactors.Actor;
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.MessageHandler;
import org.elasticsoftware.elasticactors.MethodActor;
import org.elasticsoftware.elasticactors.base.serialization.JacksonSerializationFramework;

@Actor(serializationFramework = JacksonSerializationFramework.class)
public class EchoGreetingActor extends MethodActor {

    @MessageHandler
    public void handleGreeting(ActorRef sender, Greeting message) throws Exception {
        if (message.getWho().equals("Santa Claus")) {
            sender.tell(new SpecialGreeting("Santa Claus"));
        } else {
            sender.tell(message);
        }
    }

}
