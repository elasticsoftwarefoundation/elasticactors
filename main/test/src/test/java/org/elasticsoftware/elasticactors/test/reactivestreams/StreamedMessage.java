/*
 *   Copyright 2013 - 2019 The Original Authors
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.elasticsoftware.elasticactors.test.reactivestreams;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.elasticsoftware.elasticactors.base.serialization.JacksonSerializationFramework;
import org.elasticsoftware.elasticactors.serialization.Message;

import static org.elasticsoftware.elasticactors.serialization.Message.LogFeature.CONTENTS;
import static org.elasticsoftware.elasticactors.serialization.Message.LogFeature.TIMING;

/**
 * @author Joost van de Wijgerd
 */
@Message(
    serializationFramework = JacksonSerializationFramework.class,
    immutable = true,
    durable = false,
    logBodyOnError = true,
    logOnReceive = {TIMING, CONTENTS})
public final class StreamedMessage {
    private final String key;
    private final Long sequenceNumber;

    @JsonCreator
    public StreamedMessage(@JsonProperty("key") String key,
                           @JsonProperty("sequenceNumber") Long sequenceNumber) {
        this.key = key;
        this.sequenceNumber = sequenceNumber;
    }

    public String getKey() {
        return key;
    }

    public Long getSequenceNumber() {
        return sequenceNumber;
    }
}
