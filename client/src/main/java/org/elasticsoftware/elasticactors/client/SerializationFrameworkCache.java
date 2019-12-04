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

package org.elasticsoftware.elasticactors.client;

import org.elasticsoftware.elasticactors.serialization.SerializationFramework;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public class SerializationFrameworkCache {

    private final Map<Class<? extends SerializationFramework>, SerializationFramework>
            frameworkMap = new HashMap<>();
    private final Function<Class<? extends SerializationFramework>, SerializationFramework>
            serializationFrameworkFunction;

    public SerializationFrameworkCache(
            Function<Class<? extends SerializationFramework>, SerializationFramework>
                    serializationFrameworkFunction) {
        this.serializationFrameworkFunction = serializationFrameworkFunction;
    }

    public SerializationFramework getSerializationFramework(
            Class<? extends SerializationFramework> frameworkClass) {
        return this.frameworkMap.computeIfAbsent(frameworkClass, serializationFrameworkFunction);
    }

}
