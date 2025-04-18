/*
 * Copyright 2013 - 2025 The Original Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */

package org.elasticsoftware.elasticactors.serialization.internal;

import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.cluster.ActorRefFactory;
import org.elasticsoftware.elasticactors.serialization.Deserializer;

import java.io.IOException;

/**
 * @author Joost van de Wijgerd
 */
public final class ActorRefDeserializer implements Deserializer<String,ActorRef> {
    private final ActorRefFactory actorRefFactory;

    public ActorRefDeserializer(ActorRefFactory actorRefFactory) {
        this.actorRefFactory = actorRefFactory;
    }

    @Override
    public ActorRef deserialize(String serializedObject) throws IOException {
        return actorRefFactory.create(serializedObject);
    }

    @Override
    public boolean isSafe() {
        return true;
    }
}
