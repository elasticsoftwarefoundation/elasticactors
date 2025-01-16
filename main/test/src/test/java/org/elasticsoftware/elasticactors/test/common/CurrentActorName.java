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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.elasticsoftware.elasticactors.base.serialization.JacksonSerializationFramework;
import org.elasticsoftware.elasticactors.serialization.Message;

import static org.elasticsoftware.elasticactors.serialization.Message.LogFeature.CONTENTS;
import static org.elasticsoftware.elasticactors.serialization.Message.LogFeature.TIMING;

@Message(
    serializationFramework = JacksonSerializationFramework.class,
    immutable = true,
    logBodyOnError = true,
    logOnReceive = {TIMING, CONTENTS})
public class CurrentActorName {

    private final String currentName;

    @JsonCreator
    public CurrentActorName(@JsonProperty("currentName") String currentName) {
        this.currentName = currentName;
    }

    public String getCurrentName() {
        return currentName;
    }
}
