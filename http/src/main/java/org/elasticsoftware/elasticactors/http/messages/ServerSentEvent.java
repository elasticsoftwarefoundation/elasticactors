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

package org.elasticsoftware.elasticactors.http.messages;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * @author Joost van de Wijgerd
 */
public final class ServerSentEvent {
    private final String comment;
    private final String event;
    private final List<String> data;
    private final String id;

    @JsonCreator
    public ServerSentEvent(@JsonProperty("comment")String comment,
                           @JsonProperty("event") String event,
                           @JsonProperty("data") List<String> data,
                           @JsonProperty("id") String id) {
        this.comment = comment;
        this.event = event;
        this.data = data;
        this.id = id;
    }

    @JsonProperty("comment")
    public String getComment() {
        return comment;
    }

    @JsonProperty("event")
    public String getEvent() {
        return event;
    }

    @JsonProperty("data")
    public List<String> getData() {
        return data;
    }

    @JsonProperty("id")
    public String getId() {
        return id;
    }
}
