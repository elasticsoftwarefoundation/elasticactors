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

package org.elasticsoftware.elasticactors.base.state;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.elasticsoftware.elasticactors.ActorState;
import org.elasticsoftware.elasticactors.base.serialization.JacksonSerializationFramework;
import org.elasticsoftware.elasticactors.serialization.SerializationFramework;

/**
 * @author Joost van de Wijgerd
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS)
public abstract class JacksonActorState implements ActorState<JacksonActorState> {

    @Override
    @JsonIgnore
    public final JacksonActorState getBody() {
        return this;
    }

    @JsonIgnore
    @Override
    public final Class<? extends SerializationFramework> getSerializationFramework() {
        return JacksonSerializationFramework.class;
    }
}
