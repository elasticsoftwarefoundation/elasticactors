package org.elasticsoftware.elasticactors.kafka.serialization;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.elasticsoftware.elasticactors.messaging.InternalMessage;
import org.elasticsoftware.elasticactors.serialization.internal.InternalMessageDeserializer;

import java.io.IOException;
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
                return delegate.deserialize(data);
            } catch (IOException e) {
                throw new SerializationException(e);
            }
        }
    }

    @Override
    public void close() {

    }
}
