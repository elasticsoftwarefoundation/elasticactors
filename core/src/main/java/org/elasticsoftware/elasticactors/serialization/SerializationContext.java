package org.elasticsoftware.elasticactors.serialization;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author Joost van de Wijgerd
 */
public final class SerializationContext {
    private static final ThreadLocal<IdentityHashMap<Object,ByteBuffer>> serializationCache = new ThreadLocal<>();
    private static final ThreadLocal<EvictingMap<ByteBuffer,Object>> deserializationCache = new ThreadLocal<>();
    private static final boolean deserializationCacheEnabled = Boolean.valueOf(System.getProperty("ea.deserializationCache.enabled", "false"));

    private SerializationContext() {}

    public static void initialize() {
        // only create once per thread
        if(serializationCache.get() == null) {
            serializationCache.set(new IdentityHashMap<Object, ByteBuffer>());
        }
        if(deserializationCacheEnabled && deserializationCache.get() == null) {
            deserializationCache.set(new EvictingMap<ByteBuffer, Object>());
        }
    }

    public static void reset() {
        if(serializationCache.get() != null) {
            serializationCache.get().clear();
        }
    }

    public static <T> T deserialize(final MessageDeserializer<T> deserializer,final ByteBuffer bytes) throws IOException {
        final EvictingMap<ByteBuffer, Object> deserializationCache = (deserializationCacheEnabled) ? SerializationContext.deserializationCache.get() : null;

        // see if we already have the deserialized version cached
        if(deserializationCache != null) {
            Object message = deserializationCache.get(bytes);
            if(message != null) {
                // pre cached
                return (T) message;
            }
        }
        bytes.mark();
        final T message = deserializer.deserialize(bytes);
        // check if the message is immutable
        final Message immutableAnnotation = message.getClass().getAnnotation(Message.class);
        if(immutableAnnotation != null && immutableAnnotation.immutable() && serializationCache.get() != null) {
            // reset the bytebuffer back to mark before returning
            bytes.reset();
            serializationCache.get().put(message,bytes.asReadOnlyBuffer());
            if(deserializationCache != null) {
                // this should speed up subsequent deserializations of the same bytes
                deserializationCache.put(bytes.asReadOnlyBuffer(), message);
            }
        }
        return message;
    }

    public static ByteBuffer serialize(final MessageSerializer serializer, final Object message) throws IOException {
        // check if the message is immutable
        //final Message messsageAnnotation = message.getClass().getAnnotation(Message.class);
        final Message immutableAnnotation = message.getClass().getAnnotation(Message.class);
        if(immutableAnnotation != null && immutableAnnotation.immutable() && serializationCache.get() != null) {
            final EvictingMap<ByteBuffer, Object> deserializationCache = (deserializationCacheEnabled) ? SerializationContext.deserializationCache.get() : null;
            ByteBuffer cachedBuffer = serializationCache.get().get(message);
            if(cachedBuffer != null) {
                // we have a cached version
                // return a copy
                return cachedBuffer.asReadOnlyBuffer();
            } else {
                // didn't find a buffer, but the message is immutable
                ByteBuffer serializedBuffer = serializer.serialize(message);
                // store it
                serializationCache.get().put(message,serializedBuffer.asReadOnlyBuffer());
                if(deserializationCache != null) {
                    deserializationCache.put(serializedBuffer.asReadOnlyBuffer(), message);
                }
                return serializedBuffer;
            }
        }
        // message is not immutable, just serialize it
        return serializer.serialize(message);

    }

    private static final class EvictingMap<K, V> extends LinkedHashMap<K, V> {
        private static final int MAX_SIZE = 30;

        public EvictingMap() {
            super(64,0.75f,true);
        }

        @Override
        protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
            return size() > MAX_SIZE;
        }
    }
}
