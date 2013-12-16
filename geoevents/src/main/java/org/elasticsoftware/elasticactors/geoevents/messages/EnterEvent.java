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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.geoevents.Coordinate;

import java.util.Map;

/**
 * @author Joost van de Wijgerd
 */
public final class EnterEvent {
    private final ActorRef actorRef;
    private final Coordinate location;
    private final int distanceInMetres;
    private final long lastSeen;
    private final Map<String,Object> customProperties;

    @JsonCreator
    public EnterEvent(@JsonProperty("actorRef") ActorRef actorRef,
                      @JsonProperty("location") Coordinate location,
                      @JsonProperty("distanceInMetres") int distanceInMetres,
                      @JsonProperty("lastSeen") long lastSeen,
                      @JsonProperty("customProperties") Map<String, Object> customProperties) {
        this.actorRef = actorRef;
        this.location = location;
        this.distanceInMetres = distanceInMetres;
        this.lastSeen = lastSeen;
        this.customProperties = customProperties;
    }

    @JsonProperty("actorRef")
    public ActorRef getActorRef() {
        return actorRef;
    }

    @JsonProperty("location")
    public Coordinate getLocation() {
        return location;
    }

    @JsonProperty("distanceInMetres")
    public int getDistanceInMetres() {
        return distanceInMetres;
    }

    @JsonProperty("lastSeen")
    public long getLastSeen() {
        return lastSeen;
    }

    @JsonProperty("customProperties")
    public Map<String, Object> getCustomProperties() {
        return customProperties;
    }
}
