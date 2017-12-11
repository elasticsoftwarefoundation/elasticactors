package org.elasticsoftware.elasticactors.kafka.serialization;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import org.elasticsoftware.elasticactors.messaging.InternalMessage;
import org.elasticsoftware.elasticactors.serialization.internal.InternalMessageSerializer;

import java.io.IOException;
import java.util.Map;

public final class KafkaInternalMessageSerializer implements Serializer<InternalMessage> {
    private final org.elasticsoftware.elasticactors.serialization.Serializer<InternalMessage,byte[]> delegate;

    public KafkaInternalMessageSerializer(org.elasticsoftware.elasticactors.serialization.Serializer<InternalMessage,byte[]> delegate) {
        this.delegate = delegate;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String topic, InternalMessage data) {
        try {
            return delegate.serialize(data);
        } catch (IOException e) {
            throw new SerializationException("IOException while serializing InternalMessage", e);
        }
    }

    @Override
    public void close() {

    }
}
