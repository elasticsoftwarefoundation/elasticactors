package org.elasticsoftwarefoundation.elasticactors.reflection;

import org.elasticsoftware.elasticactors.base.serialization.JacksonSerializationFramework;
import org.elasticsoftware.elasticactors.serialization.Message;

@Message(
    serializationFramework = JacksonSerializationFramework.class,
    durable = false,
    immutable = true)
public final class SerializedStateResponse {
    private final String state;

    public SerializedStateResponse(String state) {
        this.state = state;
    }

    public String getState() {
        return state;
    }
}
