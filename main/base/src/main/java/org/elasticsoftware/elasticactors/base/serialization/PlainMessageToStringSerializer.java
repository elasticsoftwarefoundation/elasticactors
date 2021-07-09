package org.elasticsoftware.elasticactors.base.serialization;

import org.elasticsoftware.elasticactors.serialization.MessageToStringSerializer;

public final class PlainMessageToStringSerializer<T> implements MessageToStringSerializer<T> {

    private final int maxLength;

    public PlainMessageToStringSerializer(int maxLength) {
        this.maxLength = maxLength;
    }

    @Override
    public String serialize(T message) throws Exception {
        String serialized = String.valueOf(message);
        if (serialized != null && serialized.length() > maxLength) {
            serialized = CONTENT_TOO_BIG_PREFIX + serialized.substring(0, maxLength);
        }
        return serialized;
    }
}
