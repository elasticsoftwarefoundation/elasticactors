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

package org.elasticsoftware.elasticactors.geoevents.messages;

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.geoevents.Coordinate;

/**
 * @author Joost van de Wijgerd
 */
public final class DeRegisterInterest {
    private final ActorRef actorRef;
    private final Coordinate location;
    private final boolean propagate;

    public DeRegisterInterest(ActorRef actorRef, Coordinate location) {
        this(actorRef, location,true);
    }

    @JsonCreator
    public DeRegisterInterest(@JsonProperty("actorRef") ActorRef actorRef,
                              @JsonProperty("location") Coordinate location,
                              @JsonProperty("propagate") boolean propagate) {
        this.actorRef = actorRef;
        this.location = location;
        this.propagate = propagate;
    }

    @JsonProperty("actorRef")
    public ActorRef getActorRef() {
        return actorRef;
    }

    @JsonProperty("location")
    public Coordinate getLocation() {
        return location;
    }

    @JsonProperty("propagate")
    public boolean isPropagate() {
        return propagate;
    }
}
