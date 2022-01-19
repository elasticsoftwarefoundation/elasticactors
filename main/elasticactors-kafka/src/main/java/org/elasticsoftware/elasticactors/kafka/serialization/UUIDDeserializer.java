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

package org.elasticsoftware.elasticactors.kafka.serialization;

import org.apache.kafka.common.serialization.Deserializer;
import org.elasticsoftware.elasticactors.messaging.UUIDTools;

import java.util.Map;
import java.util.UUID;

public final class UUIDDeserializer implements Deserializer<UUID> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public UUID deserialize(String topic, byte[] data) {
        if(data == null) {
            return null;
        } else {
            return UUIDTools.fromByteArray(data);
        }
    }

    @Override
    public void close() {

    }
}
