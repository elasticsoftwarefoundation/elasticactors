/*
 * Copyright (c) 2013 Joost van de Wijgerd <jwijgerd@gmail.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.elasterix.elasticactors;

import org.elasterix.elasticactors.serialization.Deserializer;
import org.elasterix.elasticactors.serialization.MessageDeserializer;
import org.elasterix.elasticactors.serialization.MessageSerializer;
import org.elasterix.elasticactors.serialization.Serializer;

/**
 * An {@link ActorSystem} is a collection of {@link ElasticActor} instances. {@link ElasticActor}s are persistent and
 * have an {@link ActorState}. An application implementing and {@link ActorSystem} should create classes that implement
 * the {@link ElasticActor#onMessage(Object, ActorRef)} method. Within this method the associated {@link ActorState} can
 * be obtained by calling the {@link org.elasterix.elasticactors.cluster.InternalActorStateContext#getState()} method.
 * The ElasticActors framework will take care of persisting the state. The application can control the serialization and
 * deserialization by providing appropriate {@link Deserializer} in the {@link org.elasterix.elasticactors.ActorSystem#getActorStateDeserializer()}
 *
 * @author Joost van de Wijgerd
 */
public interface ActorSystem {
    /**
     * The name of this {@link ActorSystem}. The name has to be unique within the same cluster
     *
     * @return
     */
    String getName();

    <T> ActorRef actorOf(String actorId, Class<T> actorClass);

    ActorRef actorFor(String actorId);

}
