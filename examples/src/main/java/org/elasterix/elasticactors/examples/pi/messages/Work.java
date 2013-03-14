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
public class Work {
    private final int start;
    private final int nrOfElements;

    @JsonCreator
    public Work(@JsonProperty("start") int start, @JsonProperty("nrOfElements") int nrOfElements) {
        this.start = start;
        this.nrOfElements = nrOfElements;
    }

    @JsonProperty("start")
    public int getStart() {
        return start;
    }

    @JsonProperty("nrOfElements")
    public int getNrOfElements() {
        return nrOfElements;
    }
}
