/*
 * Copyright 2013 - 2024 The Original Authors
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

package org.elasticsoftware.elasticactors.base.actors;

import org.elasticsoftware.elasticactors.Actor;
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.UntypedActor;
import org.elasticsoftware.elasticactors.base.serialization.JacksonSerializationFramework;
import org.elasticsoftware.elasticactors.base.state.AliasActorState;
import org.elasticsoftware.elasticactors.state.PersistenceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.elasticsoftware.elasticactors.state.ActorLifecycleStep.CREATE;

/**
 * Actor that can be used to create and Alias for an Actor. Handy if you need an actor to be available under multiple
 * (unique) keys. All messages will be forwarded to the Aliased {@link ActorRef} keeping the original sender thus leaving
 * the {@link AliasActor} as an opaque entity in the system.
 *
 * The {@link AliasActorState} will only be persisted in the {@link org.elasticsoftware.elasticactors.state.ActorLifecycleStep#CREATE}
 * lifecycle step since the state is immutable
 *
 * @author Joost van de Wijgerd
 */
@Actor(stateClass = AliasActorState.class, serializationFramework = JacksonSerializationFramework.class)
@PersistenceConfig(persistOnMessages = false,persistOn = {CREATE})
public final class AliasActor extends UntypedActor {

    private final static Logger staticLogger = LoggerFactory.getLogger(AliasActor.class);

    @Override
    protected Logger initLogger() {
        return staticLogger;
    }

    /**
     * Simply pass the message on to the {@link org.elasticsoftware.elasticactors.base.state.AliasActorState#getAliasedActor()}
     *
     * @param sender        the sender of the message (as passed in {@link ActorRef#tell(Object, ActorRef)})
     * @param message       the message object
     * @throws Exception
     */
    @Override
    public void onReceive(ActorRef sender, Object message) throws Exception {
        getState(AliasActorState.class).getAliasedActor().tell(message, sender);
    }
}
