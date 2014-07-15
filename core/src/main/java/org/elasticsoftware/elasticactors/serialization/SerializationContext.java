package org.elasticsoftware.elasticactors.serialization;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.IdentityHashMap;

/**
 * @author Joost van de Wijgerd
 */
public final class SerializationContext {
    private static final Logger logger = Logger.getLogger(SerializationContext.class);
    private static final ThreadLocal<IdentityHashMap<Object,ByteBuffer>> map = new ThreadLocal<>();

    private SerializationContext() {}

    public static void initialize() {
        // only create once per thread
        if(map.get() == null) {
            map.set(new IdentityHashMap<Object, ByteBuffer>());
        }
    }

    public static void reset() {
        if(map.get() != null) {
            map.get().clear();
        }
    }

    public static <T> T deserialize(MessageDeserializer<T> deserializer,final ByteBuffer bytes) throws IOException {
        bytes.mark();
        final T message = deserializer.deserialize(bytes);
        // check if the message is immutable
        final Message immutableAnnotation = message.getClass().getAnnotation(Message.class);
        if(immutableAnnotation != null && immutableAnnotation.immutable() && map.get() != null) {
            // reset the bytebuffer back to mark before returning
            bytes.reset();
            map.get().put(message,bytes.asReadOnlyBuffer());
            /*if(logger.isDebugEnabled()) {
                logger.debug(format("Thread [%s] (deserialize) Cached message bytes for message %s@%d",Thread.currentThread().getName(),message.getClass().getSimpleName(),System.identityHashCode(message)));
            }*/
        }
        return message;
    }

    public static ByteBuffer serialize(final MessageSerializer serializer, final Object message) throws IOException {
        // check if the message is immutable
        //final Message messsageAnnotation = message.getClass().getAnnotation(Message.class);
        final Message immutableAnnotation = message.getClass().getAnnotation(Message.class);
        if(immutableAnnotation != null && immutableAnnotation.immutable() && map.get() != null) {
            ByteBuffer cachedBuffer = map.get().get(message);
            if(cachedBuffer != null) {
                // we have a cached version
                /*if(logger.isDebugEnabled()) {
                    logger.debug(format("Thread [%s] (serialize) Cache HIT for message %s@%d",Thread.currentThread().getName(),message.getClass().getSimpleName(),System.identityHashCode(message)));
                }*/
                // return a copy
                return cachedBuffer.asReadOnlyBuffer();
            } else {
                // didn't find a buffer, but the message is immutable
                ByteBuffer serializedBuffer = serializer.serialize(message);
                // store it
                map.get().put(message,serializedBuffer.asReadOnlyBuffer());
                /*if(logger.isDebugEnabled()) {
                    logger.debug(format("Thread [%s] (serialize) Cached message bytes for message %s@%d",Thread.currentThread().getName(),message.getClass().getSimpleName(),System.identityHashCode(message)));
                }*/
                return serializedBuffer;
            }
        }
        // message is not immutable, just serialize it
        return serializer.serialize(message);

    }
}
