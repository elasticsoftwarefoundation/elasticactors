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

package org.elasticsoftware.elasticactors.base.serialization;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdScalarDeserializer;
import org.elasticsoftware.elasticactors.cluster.scheduler.ScheduledMessageRefFactory;
import org.elasticsoftware.elasticactors.scheduler.ScheduledMessageRef;

import java.io.IOException;

/**
 *
 */
public final class JacksonScheduledMessageRefDeserializer extends StdScalarDeserializer<ScheduledMessageRef> {
    private final ScheduledMessageRefFactory scheduledMessageRefFactory;

    public JacksonScheduledMessageRefDeserializer(ScheduledMessageRefFactory scheduledMessageRefFactory) {
        super(ScheduledMessageRef.class);
        this.scheduledMessageRefFactory = scheduledMessageRefFactory;
    }

    @Override
    public ScheduledMessageRef deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException {
        JsonToken curr = jp.getCurrentToken();
        // Usually should just get string value:
        if (curr == JsonToken.VALUE_STRING) {
            return scheduledMessageRefFactory.create(jp.getText());
        }
        throw ctxt.mappingException(_valueClass, curr);
    }
}
