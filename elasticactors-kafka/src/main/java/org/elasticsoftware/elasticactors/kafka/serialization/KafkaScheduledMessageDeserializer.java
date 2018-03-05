package org.elasticsoftware.elasticactors.kafka.serialization;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.elasticsoftware.elasticactors.cluster.scheduler.ScheduledMessage;
import org.elasticsoftware.elasticactors.serialization.internal.ScheduledMessageDeserializer;

import java.io.IOException;
import java.util.Map;

public final class KafkaScheduledMessageDeserializer implements Deserializer<ScheduledMessage> {
    private final ScheduledMessageDeserializer delegate;

    public KafkaScheduledMessageDeserializer(ScheduledMessageDeserializer delegate) {
        this.delegate = delegate;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public ScheduledMessage deserialize(String topic, byte[] data) {
        try {
            return delegate.deserialize(data);
        } catch (IOException e) {
            throw new SerializationException(e);
        }
    }


    @Override
    public void close() {

    }
}
