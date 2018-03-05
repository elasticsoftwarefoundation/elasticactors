package org.elasticsoftware.elasticactors.kafka.serialization;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.elasticsoftware.elasticactors.cluster.ActorSystemEventListener;
import org.elasticsoftware.elasticactors.serialization.internal.ActorSystemEventListenerDeserializer;

import java.io.IOException;
import java.util.Map;

public final class KafkaActorSystemEventListenerDeserializer implements Deserializer<ActorSystemEventListener> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public ActorSystemEventListener deserialize(String topic, byte[] data) {
        try {
            return ActorSystemEventListenerDeserializer.get().deserialize(data);
        } catch (IOException e) {
            throw new SerializationException(e);
        }
    }

    @Override
    public void close() {

    }
}
