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

package org.elasticsoftware.elasticactors.examples.pi.messages;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.elasticsoftware.elasticactors.base.serialization.JacksonSerializationFramework;
import org.elasticsoftware.elasticactors.serialization.Message;

/**
 * @author Joost van de Wijgerd
 */
@Message(serializationFramework = JacksonSerializationFramework.class)
public final class Work {
    private final int start;
    private final int nrOfElements;
    private final String calculationId;

    @JsonCreator
    public Work(@JsonProperty("start") int start, @JsonProperty("nrOfElements") int nrOfElements, @JsonProperty("calculationId") String calculationId) {
        this.start = start;
        this.nrOfElements = nrOfElements;
        this.calculationId = calculationId;
    }

    @JsonProperty("start")
    public int getStart() {
        return start;
    }

    @JsonProperty("nrOfElements")
    public int getNrOfElements() {
        return nrOfElements;
    }

    @JsonProperty("calculationId")
    public String getCalculationId() {
        return calculationId;
    }
}
