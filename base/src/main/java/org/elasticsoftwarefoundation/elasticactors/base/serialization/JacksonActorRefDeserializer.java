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

package org.elasticsoftwarefoundation.elasticactors.base.serialization;

import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.JsonToken;
import org.codehaus.jackson.map.DeserializationContext;
import org.codehaus.jackson.map.JsonDeserializer;
import org.codehaus.jackson.map.deser.std.StdScalarDeserializer;
import org.elasterix.elasticactors.ActorRef;
import org.elasterix.elasticactors.cluster.ActorRefFactory;
import org.elasterix.elasticactors.serialization.Deserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Configurable;

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
    public ActorRef deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException, JsonProcessingException {
        JsonToken curr = jp.getCurrentToken();
        // Usually should just get string value:
        if (curr == JsonToken.VALUE_STRING) {
            return actorRefFactory.create(jp.getText());
        }
        throw ctxt.mappingException(_valueClass, curr);
    }
}
