package org.elasticsoftware.elasticactors.kafka.serialization;

import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.elasticsoftware.elasticactors.ShardKey;
import org.elasticsoftware.elasticactors.cluster.scheduler.ScheduledMessage;
import org.elasticsoftware.elasticactors.messaging.InternalMessage;
import org.elasticsoftware.elasticactors.serialization.internal.ScheduledMessageSerializer;
import org.elasticsoftware.elasticactors.state.PersistentActor;

import java.util.Map;
import java.util.UUID;

import static java.lang.String.format;

public final class KafkaProducerSerializer implements Serializer<Object> {
    private final UUIDSerializer uuidSerializer = new UUIDSerializer();
    private final StringSerializer stringSerializer = new StringSerializer();
    private final KafkaInternalMessageSerializer internalMessageSerializer;
    private final KafkaPersistentActorSerializer persistentActorSerializer;
    private boolean isKey = false;

    public KafkaProducerSerializer(KafkaInternalMessageSerializer internalMessageSerializer,
                                   KafkaPersistentActorSerializer persistentActorSerializer) {
        this.internalMessageSerializer = internalMessageSerializer;
        this.persistentActorSerializer = persistentActorSerializer;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        this.isKey = isKey;
    }

    @Override
    public byte[] serialize(String topic, Object data) {
        if(isKey) {
            if (data instanceof UUID) {
                return uuidSerializer.serialize(topic, (UUID) data);
            } else if (data instanceof String) {
                return stringSerializer.serialize(topic, (String) data);
            } else {
                throw new IllegalArgumentException(format("Key of type %s is not supported by this Serializer", data.getClass().getName()));
            }
        } else {
            if (data instanceof InternalMessage) {
                return internalMessageSerializer.serialize(topic, (InternalMessage) data);
            } else if (data instanceof byte[]) {
                return (byte[]) data;
            } else if(data instanceof ScheduledMessage) {
                return ScheduledMessageSerializer.get().serialize((ScheduledMessage) data);
            } else if (data instanceof PersistentActor) {
                return persistentActorSerializer.serialize(topic, (PersistentActor<ShardKey>) data);
            } else {
                throw new IllegalArgumentException(format("Value of type %s is not supported by this Serializer", data.getClass().getName()));
            }
        }
    }

    @Override
    public void close() {

    }
}
