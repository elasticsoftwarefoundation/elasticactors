/*
 * Copyright 2013 - 2023 The Original Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */

package org.elasticsoftware.elasticactors.kafka.serialization;

import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.elasticsoftware.elasticactors.ShardKey;
import org.elasticsoftware.elasticactors.cluster.ActorSystemEventListener;
import org.elasticsoftware.elasticactors.cluster.scheduler.ScheduledMessage;
import org.elasticsoftware.elasticactors.messaging.InternalMessage;
import org.elasticsoftware.elasticactors.serialization.internal.ActorSystemEventListenerSerializer;
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

    public KafkaProducerSerializer(KafkaInternalMessageSerializer internalMessageSerializer,
                                   KafkaPersistentActorSerializer persistentActorSerializer) {
        this.internalMessageSerializer = internalMessageSerializer;
        this.persistentActorSerializer = persistentActorSerializer;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public byte[] serialize(String topic, Object data) {
        if(data == null) {
            return null;
        } else if (data instanceof UUID) {
            return uuidSerializer.serialize(topic, (UUID) data);
        } else if (data instanceof String) {
            return stringSerializer.serialize(topic, (String) data);
        } /*else {
            throw new IllegalArgumentException(format("Key of type %s is not supported by this Serializer", data.getClass().getName()));
        } */else if (data instanceof InternalMessage) {
            return internalMessageSerializer.serialize(topic, (InternalMessage) data);
        } else if (data instanceof byte[]) {
            return (byte[]) data;
        } else if(data instanceof ScheduledMessage) {
            return ScheduledMessageSerializer.get().serialize((ScheduledMessage) data);
        } else if(data instanceof ActorSystemEventListener) {
            return ActorSystemEventListenerSerializer.get().serialize((ActorSystemEventListener) data);
        } else if (data instanceof PersistentActor) {
            return persistentActorSerializer.serialize(topic, (PersistentActor<ShardKey>) data);
        } else {
            throw new IllegalArgumentException(format("Keys nor Values of type %s are supported by this Serializer", data.getClass().getName()));
        }
    }

    @Override
    public void close() {

    }
}
