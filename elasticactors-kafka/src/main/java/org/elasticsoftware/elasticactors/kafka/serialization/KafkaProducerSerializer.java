package org.elasticsoftware.elasticactors.kafka.serialization;

import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.elasticsoftware.elasticactors.ShardKey;
import org.elasticsoftware.elasticactors.messaging.InternalMessage;
import org.elasticsoftware.elasticactors.state.PersistentActor;

import java.util.Map;
import java.util.UUID;

import static java.lang.String.format;

public final class KafkaProducerSerializer implements Serializer<Object> {
    private final UUIDSerializer uuidSerializer = new UUIDSerializer();
    private final StringSerializer stringSerializer = new StringSerializer();
    private final KafkaInternalMessageSerializer internalMessageSerializer;
    private final KafkaPersistentActorSerializer persistentActorSerializer;

    public KafkaProducerSerializer(KafkaInternalMessageSerializer internalMessageSerializer,
                                   KafkaPersistentActorSerializer persistentActorSerializer) {
        this.internalMessageSerializer = internalMessageSerializer;
        this.persistentActorSerializer = persistentActorSerializer;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // don't care if it's a key or a value
    }

    @Override
    public byte[] serialize(String topic, Object data) {
        if(data instanceof UUID) {
            return uuidSerializer.serialize(topic, (UUID) data);
        } else if(data instanceof String) {
            return stringSerializer.serialize(topic, (String) data);
        } else if(data instanceof PersistentActor) {
            return persistentActorSerializer.serialize(topic, (PersistentActor<ShardKey>) data);
        } else if(data instanceof InternalMessage) {
            return internalMessageSerializer.serialize(topic, (InternalMessage) data);
        } else {
            throw new IllegalArgumentException(format("data of type %s is not supported by this Serializer", data.getClass().getName()));
        }
    }

    @Override
    public void close() {

    }
}
