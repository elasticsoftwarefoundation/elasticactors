/*
 * Copyright 2013 the original authors
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

import java.util.Map;

/**
 * @author Joost van de Wijgerd
 */
public interface ActorStateFactory {
    /**
     * @return  a new empty ActorState instance
     */
    ActorState create();

    ActorState create(Map<String,Object> map);

    /**
     * Creates an ActorState based on a pojo. The pojo should be serializable with the configured
     *  {@link org.elasticsoftware.elasticactors.ActorSystemConfiguration#getActorStateDeserializer()} and
     *  {@link org.elasticsoftware.elasticactors.ActorSystemConfiguration#getActorStateSerializer()}
     *
     * @param backingObject
     * @return
     */
    ActorState create(Object backingObject);
}
