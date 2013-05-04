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

package org.elasticsoftware.elasticactors.serialization.internal;

import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.cluster.ActorRefFactory;
import org.elasticsoftware.elasticactors.serialization.Deserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Configurable;

import java.io.IOException;

/**
 * @author Joost van de Wijgerd
 */
@Configurable
public final class ActorRefDeserializer implements Deserializer<String,ActorRef> {
    private static final ActorRefDeserializer INSTANCE = new ActorRefDeserializer();
    private ActorRefFactory actorRefFactory;

    public static ActorRefDeserializer get() {
        return INSTANCE;
    }

    @Override
    public ActorRef deserialize(String serializedObject) throws IOException {
        return actorRefFactory.create(serializedObject);
    }

    @Autowired
    public void setActorRefFactory(ActorRefFactory actorRefFactory) {
        this.actorRefFactory = actorRefFactory;
    }
}
