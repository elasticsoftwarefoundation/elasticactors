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
            return UUIDTools.toUUID(data);
        }
    }

    @Override
    public void close() {

    }
}
