package org.elasticsoftware.elasticactors.cluster;

import org.elasticsoftware.elasticactors.serialization.MessageDeserializer;
import org.elasticsoftware.elasticactors.serialization.MessageSerializer;

public interface SerializationRegistry {
    /**
     * Return the serializer for the given message type
     *
     * @param messageClass
     * @param <T>
     * @return
     */
    <T> MessageSerializer<T> getSerializer(Class<T> messageClass);

    /**
     * Return the deserializer for the give message type
     *
     * @param messageClass
     * @param <T>
     * @return
     */
    <T> MessageDeserializer<T> getDeserializer(Class<T> messageClass);
}
