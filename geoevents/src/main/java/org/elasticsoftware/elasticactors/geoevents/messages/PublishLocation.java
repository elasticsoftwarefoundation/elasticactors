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

package org.elasticsoftware.elasticactors.geoevents.messages;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.base.serialization.JacksonSerializationFramework;
import org.elasticsoftware.elasticactors.geoevents.Coordinate;
import org.elasticsoftware.elasticactors.serialization.Message;

import java.util.Map;

/**
 * @author Joost van de Wijgerd
 */
@Message(serializationFramework = JacksonSerializationFramework.class,durable = true)
public final class PublishLocation {
    private final ActorRef ref;
    private final Coordinate location;
    private final long ttlInSeconds;
    private final Map<String,Object> customProperties;

    @JsonCreator
    public PublishLocation(@JsonProperty("ref") ActorRef ref,
                           @JsonProperty("location") Coordinate location,
                           @JsonProperty("ttlInSeconds") long ttlInSeconds,
                           @JsonProperty("customProperties") Map<String, Object> customProperties) {
        this.ref = ref;
        this.location = location;
        this.ttlInSeconds = ttlInSeconds;
        this.customProperties = customProperties;
    }

    @JsonProperty("ref")
    public ActorRef getRef() {
        return ref;
    }

    @JsonProperty("location")
    public Coordinate getLocation() {
        return location;
    }

    @JsonProperty("ttlInSeconds")
    public long getTtlInSeconds() {
        return ttlInSeconds;
    }

    @JsonProperty("customProperties")
    public Map<String, Object> getCustomProperties() {
        return customProperties;
    }
}
