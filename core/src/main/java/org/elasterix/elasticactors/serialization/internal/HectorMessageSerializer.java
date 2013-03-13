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

package org.elasterix.elasticactors.serialization.internal;

import me.prettyprint.cassandra.serializers.SerializerTypeInferer;
import me.prettyprint.hector.api.Serializer;
import org.elasterix.elasticactors.serialization.MessageSerializer;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Delegate to Hector serializer framework for primitive types
 *
 * @author Joost van de Wijgerd
 */
public final class HectorMessageSerializer<T> implements MessageSerializer<T> {
    private final Serializer<T> hectorSerializer;

    public HectorMessageSerializer(Serializer<T> hectorSerializer) {
        this.hectorSerializer = hectorSerializer;
    }

    @Override
    public ByteBuffer serialize(T object) throws IOException {
        return hectorSerializer.toByteBuffer(object);
    }
}
