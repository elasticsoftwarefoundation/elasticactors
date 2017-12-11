package org.elasticsoftware.elasticactors.kafka.serialization;

import org.apache.kafka.common.serialization.Serializer;
import org.elasticsoftware.elasticactors.messaging.UUIDTools;

import java.util.Map;
import java.util.UUID;

public final class UUIDSerializer implements Serializer<UUID> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String topic, UUID uuid) {
        if (uuid == null) {
            return null;
        } else {
            return UUIDTools.toByteArray(uuid);
        }
    }

    @Override
    public void close() {

    }
}
