package org.elasticsoftware.elasticactors.serialization;

import javax.annotation.concurrent.Immutable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.IdentityHashMap;

/**
 * @author Joost van de Wijgerd
 */
public final class SerializationContext {
    private static final ThreadLocal<IdentityHashMap<Object,ByteBuffer>> map = new ThreadLocal<>();

    private SerializationContext() {}

    public static void initialize() {
        map.set(new IdentityHashMap<Object, ByteBuffer>());
    }

    public static void reset() {
        map.set(null);
    }

    public static Object deserialize(MessageDeserializer<?> deserializer,final ByteBuffer bytes) throws IOException {
        bytes.mark();
        final Object message = deserializer.deserialize(bytes);
        // check if the message is immutable
        //final Message messsageAnnotation = message.getClass().getAnnotation(Message.class);
        final Immutable immutableAnnotation = message.getClass().getAnnotation(Immutable.class);
        if(immutableAnnotation != null && map.get() != null) {
            // reset the bytebuffer back to mark before returning
            bytes.reset();
            map.get().put(message,bytes.asReadOnlyBuffer());
        }
        return message;
    }

    public static ByteBuffer serialize(final MessageSerializer serializer, final Object message) throws IOException {
        // check if the message is immutable
        //final Message messsageAnnotation = message.getClass().getAnnotation(Message.class);
        final Immutable immutableAnnotation = message.getClass().getAnnotation(Immutable.class);
        if(immutableAnnotation != null && map.get() != null) {
            ByteBuffer cachedBuffer = map.get().get(message);
            if(cachedBuffer != null) {
                // we have a cached version
                // reset first
                cachedBuffer.reset();
                // and return
                return cachedBuffer;
            } else {
                // didn't find a buffer, but the message is immutable
                ByteBuffer serializedBuffer = serializer.serialize(message);
                // store it
                map.get().put(message,serializedBuffer.asReadOnlyBuffer());
                return serializedBuffer;
            }
        }
        // message is not immutable, just serialize it
        return serializer.serialize(message);

    }
}
