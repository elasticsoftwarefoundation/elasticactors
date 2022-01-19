/*
 *   Copyright 2013 - 2022 The Original Authors
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
import org.elasticsoftware.elasticactors.ManagedActor;
import org.elasticsoftware.elasticactors.MessageHandler;
import org.elasticsoftware.elasticactors.MethodActor;
import org.elasticsoftware.elasticactors.base.serialization.JacksonSerializationFramework;
import org.elasticsoftware.elasticactors.state.PersistenceConfig;

@ManagedActor(
        value = {
                ManagedNameActor.ACTOR_ID_0,
                ManagedNameActor.ACTOR_ID_1,
                ManagedNameActor.ACTOR_ID_2
        },
        initialStateProvider = InitialNameStateProvider.class,
        exclusive = false)
@Actor(
        serializationFramework = JacksonSerializationFramework.class,
        stateClass = NameActorState.class)
@PersistenceConfig(excluded = GetActorName.class)
public class ManagedNameActor extends MethodActor {

    public static final String ACTOR_ID_0 = "managedNameActor0";
    public static final String ACTOR_ID_1 = "managedNameActor1";
    public static final String ACTOR_ID_2 = "managedNameActor2";

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
