/*
 * Copyright 2013 - 2025 The Original Authors
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

package org.elasticsoftware.elasticactors.test.common;

import org.elasticsoftware.elasticactors.*;
import org.elasticsoftware.elasticactors.base.serialization.JacksonSerializationFramework;
import org.elasticsoftware.elasticactors.state.PersistenceConfig;

@SingletonActor(SingletonNameActor.ACTOR_ID)
@Actor(
        serializationFramework = JacksonSerializationFramework.class,
        stateClass = NameActorState.class)
@PersistenceConfig(excluded = GetActorName.class)
public class SingletonNameActor extends MethodActor {

    public final static String ACTOR_ID = "testSingletonActor";

    @MessageHandler
    public void handleSetActorName(
            ActorRef sender,
            SetActorName setActorName,
            NameActorState state) {
        logger.info(
                "Changing actor name. Old: '{}'. New: '{}'",
                state.getName(),
                setActorName.getNewName());
        CurrentActorName currentActorName = new CurrentActorName(state.getName());
        state.setName(setActorName.getNewName());
        if (sender != null) {
            sender.tell(currentActorName);
        }
    }

    @MessageHandler
    public void handleGetActorName(
            ActorRef sender,
            GetActorName getActorName,
            NameActorState state) {
        sender.tell(new CurrentActorName(state.getName()));
    }

}
