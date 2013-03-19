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

package org.elasterix.elasticactors.examples.pi.messages;

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * @author Joost van de Wijgerd
 */
public final class PiApproximation {
    private final double pi;
    private final long duration;

    @JsonCreator
    public PiApproximation(@JsonProperty("pi") double pi, @JsonProperty("duration") long duration) {
        this.pi = pi;
        this.duration = duration;
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
