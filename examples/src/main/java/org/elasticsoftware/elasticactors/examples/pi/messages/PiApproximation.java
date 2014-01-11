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
public final class PiApproximation {
    private final double pi;
    private final long duration;
    private final String calculationId;

    @JsonCreator
    public PiApproximation(@JsonProperty("calculationId") String calculationId,
                           @JsonProperty("pi") double pi,
                           @JsonProperty("duration") long duration) {
        this.calculationId = calculationId;
        this.pi = pi;
        this.duration = duration;
    }

    @JsonProperty("calculationId")
    public String getCalculationId() {
        return calculationId;
    }

    @JsonProperty("pi")
    public double getPi() {
        return pi;
    }

    @JsonProperty("duration")
    public long getDuration() {
        return duration;
    }
}
