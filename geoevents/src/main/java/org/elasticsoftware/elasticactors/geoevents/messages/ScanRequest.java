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
import org.elasticsoftware.elasticactors.base.serialization.JacksonSerializationFramework;
import org.elasticsoftware.elasticactors.geoevents.Coordinate;
import org.elasticsoftware.elasticactors.serialization.Message;

import java.util.UUID;

/**
 * @author Joost van de Wijgerd
 */
@Message(serializationFramework = JacksonSerializationFramework.class,durable = false)
public final class ScanRequest {
    private final UUID id;
    private final Coordinate location;
    private final int radiusInMetres;

    public ScanRequest(Coordinate location, int radiusInMetres) {
        this(UUID.randomUUID(),location,radiusInMetres);
    }

    @JsonCreator
    public ScanRequest(@JsonProperty("id") UUID id,@JsonProperty("location") Coordinate location, @JsonProperty("radiusInMetres") int radiusInMetres) {
        this.id = id;
        this.location = location;
        this.radiusInMetres = radiusInMetres;
    }

    @JsonProperty("id")
    public UUID getId() {
        return id;
    }

    @JsonProperty("location")
    public Coordinate getLocation() {
        return location;
    }

    @JsonProperty("radiusInMetres")
    public int getRadiusInMetres() {
        return radiusInMetres;
    }
}
