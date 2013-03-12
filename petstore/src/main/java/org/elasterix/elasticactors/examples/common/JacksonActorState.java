/*
 * Copyright 2013 Joost van de Wijgerd
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

package org.elasterix.elasticactors.examples.common;

import org.codehaus.jackson.map.ObjectMapper;
import org.elasterix.elasticactors.ActorState;

import java.util.Map;

/**
 *
 */
public final class JacksonActorState implements ActorState {
    private final ObjectMapper objectMapper;
    private final Map<String,Object> stateMap;
    private Object stateObject;

    public JacksonActorState(ObjectMapper objectMapper, Map<String, Object> stateMap) {
        this.objectMapper = objectMapper;
        this.stateMap = stateMap;
    }

    @Override
    public Map<String, Object> getAsMap() {
        return stateMap;
    }

    @Override
    public <T> T getAsObject(Class<T> objectClass) {
        if(stateObject == null) {
            stateObject = objectMapper.convertValue(stateMap,objectClass);
        }
        return (T) stateObject;
    }
}
