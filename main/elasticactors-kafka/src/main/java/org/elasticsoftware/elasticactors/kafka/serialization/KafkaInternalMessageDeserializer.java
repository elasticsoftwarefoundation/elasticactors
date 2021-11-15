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

package org.elasticsoftware.elasticactors.kafka.serialization;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.elasticsoftware.elasticactors.messaging.InternalMessage;
import org.elasticsoftware.elasticactors.serialization.internal.InternalMessageDeserializer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

public final class KafkaInternalMessageDeserializer implements Deserializer<InternalMessage> {
    private final InternalMessageDeserializer delegate;

    public KafkaInternalMessageDeserializer(InternalMessageDeserializer delegate) {
        this.delegate = delegate;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public InternalMessage deserialize(String topic, byte[] data) {
        if(data == null) {
            return null;
        } else {
            try {
                return delegate.deserialize(ByteBuffer.wrap(data));
            } catch (IOException e) {
                throw new SerializationException(e);
            }
        }
    }

    @Override
    public void close() {

    }
}
