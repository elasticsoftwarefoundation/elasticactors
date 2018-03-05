package org.elasticsoftware.elasticactors.kafka.serialization;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import org.elasticsoftware.elasticactors.ShardKey;
import org.elasticsoftware.elasticactors.state.PersistentActor;

import java.io.IOException;
import java.util.Map;

public final class KafkaPersistentActorSerializer implements Serializer<PersistentActor<ShardKey>> {
    private final org.elasticsoftware.elasticactors.serialization.Serializer<PersistentActor<ShardKey>,byte[]> delegate;

    public KafkaPersistentActorSerializer(org.elasticsoftware.elasticactors.serialization.Serializer<PersistentActor<ShardKey>, byte[]> delegate) {
        this.delegate = delegate;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String topic, PersistentActor<ShardKey> data) {
        try {
            return delegate.serialize(data);
        } catch (IOException e) {
            throw new SerializationException("IOException while serializing PersistentActor", e);
        }
    }

    @Override
    public void close() {

    }
}
