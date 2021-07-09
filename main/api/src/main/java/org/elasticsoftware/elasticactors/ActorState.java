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

package org.elasticsoftware.elasticactors;

import org.elasticsoftware.elasticactors.serialization.SerializationFramework;

/**
 * Tagging interface for the state object of an {@link ElasticActor}. The interface has a generic
 * type Body that should normally be the same type as the implementing class. The reason behind this
 * is that when for instance Jackson (JSON) is used as the {@link org.elasticsoftware.elasticactors.serialization.SerializationFramework}
 * there can be generic support built-in. When a concrete class cannot be found there can be a fallback
 * implemented as ActorState&lt;Map&lt;String,Object&gt;&gt;
 *
 * @author Joost van de Wijgerd
 */
public interface ActorState<Body> {
    /**
     * The body (the actual state)
     *
     * @return the body (the actual state)
     */
    Body getBody();

    /**
     * The type of the SerializationFramework that should be used to serialize this state class
     *
     * @return the type of the SerializationFramework that should be used to serialize this state class
     */
    Class<? extends SerializationFramework> getSerializationFramework();
}

