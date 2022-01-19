/*
 *   Copyright 2013 - 2022 The Original Authors
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
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.cluster.ActorRefFactory;

import java.io.IOException;

/**
 *
 */
public final class JacksonActorRefDeserializer extends StdScalarDeserializer<ActorRef> {
    private final ActorRefFactory actorRefFactory;

    public JacksonActorRefDeserializer(ActorRefFactory actorRefFactory) {
        super(ActorRef.class);
        this.actorRefFactory = actorRefFactory;
    }

    @Override
    public ActorRef deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException {
        JsonToken curr = jp.getCurrentToken();
        // Usually should just get string value:
        if (curr == JsonToken.VALUE_STRING) {
            return actorRefFactory.create(jp.getText());
        }
        throw ctxt.mappingException(_valueClass, curr);
    }
}
