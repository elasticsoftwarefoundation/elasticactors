/*
 * Copyright 2013 - 2014 The Original Authors
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

package org.elasticsoftware.elasticactors;

import org.elasticsoftware.elasticactors.serialization.NoopSerializationFramework;
import org.elasticsoftware.elasticactors.serialization.SerializationFramework;
import org.elasticsoftware.elasticactors.state.NullActorState;

import java.lang.annotation.*;

/**
 * Use this annotation to mark a class as an Actor. The runtime uses this to determine the
 * {@link SerializationFramework} and the {@link ActorState} class. An Actor class that is
 * not annotated with this annotation (or {@link TempActor} or {@link ServiceActor}) is not
 * considered to be valid by the runtime even though it implements {@link ElasticActor}
 *
 * ActorState defaults to NullActorState
 * SerializationFramework default to NoopSerializationFramework
 *
 * @author Joost van de Wijgerd
 * @author Leonard Wolters
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface Actor {
    /**
     * The class that implements the {@link ActorState} for this Actor
     *
     * @return
     */
    Class<? extends ActorState> stateClass() default NullActorState.class;

    /**
     * The {@link SerializationFramework} used to serialize and deserialize the {@link ActorState}
     *
     * @return
     */
    Class<? extends SerializationFramework> serializationFramework() default NoopSerializationFramework.class;
}
