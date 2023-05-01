package org.elasticsoftwarefoundation.elasticactors.reflection;

import org.elasticsoftware.elasticactors.base.serialization.JacksonSerializationFramework;
import org.elasticsoftware.elasticactors.serialization.Message;

@Message(
    serializationFramework = JacksonSerializationFramework.class,
    durable = false,
    immutable = true)
public final class SerializedStateRequest {
    public static final SerializedStateRequest INSTANCE = new SerializedStateRequest();

    private SerializedStateRequest() {
    }
}
