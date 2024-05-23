/*
 * Copyright 2013 - 2024 The Original Authors
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

package org.elasticsoftware.elasticactors.serialization;

import org.elasticsoftware.elasticactors.util.ByteBufferUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author Joost van de Wijgerd
 */
public final class SerializationContext {
    private static final ThreadLocal<IdentityHashMap<Object,ByteBuffer>> serializationCache = ThreadLocal.withInitial(IdentityHashMap::new);
    private static final ThreadLocal<EvictingMap<DeserializationKey,Object>> deserializationCache = ThreadLocal.withInitial(EvictingMap::new);
    // keypool to avoid too much garbage being generated
    private static final ThreadLocal<DeserializationKey> keyPool = ThreadLocal.withInitial(() -> new DeserializationKey(null,null));
    private static final boolean deserializationCacheEnabled = Boolean.parseBoolean(System.getProperty("ea.deserializationCache.enabled", "false"));
    private static final boolean serializationCacheEnabled = Boolean.parseBoolean(System.getProperty("ea.serializationCache.enabled", "false"));

    private SerializationContext() {}

    public static void reset() {
        if (serializationCacheEnabled) {
            serializationCache.get().clear();
        }
    }

    public static <T> T deserialize(MessageDeserializer<T> deserializer, ByteBuffer bytes)
        throws IOException
    {
        if (deserializationCacheEnabled || serializationCacheEnabled) {
            return deserializeWithCache(deserializer, bytes);
        } else {
            return deserializeWithoutCache(deserializer, bytes);
        }
    }

    private static <T> T deserializeWithoutCache(
        MessageDeserializer<T> deserializer,
        ByteBuffer bytes)
        throws IOException
    {
        if (deserializer.isSafe()) {
            return deserializer.deserialize(bytes);
        } else {
            return ByteBufferUtils.throwingApplyAndReset(bytes, deserializer::deserialize);
        }
    }

    private static <T> T deserializeWithCache(MessageDeserializer<T> deserializer, ByteBuffer bytes)
        throws IOException
    {
        Map<DeserializationKey, Object> deserializationCache = deserializationCacheEnabled
            ? SerializationContext.deserializationCache.get()
            : null;

        // see if we already have the deserialized version cached
        if (deserializationCache != null) {
            DeserializationKey deserializationKey = keyPool.get();
            deserializationKey.populate(deserializer.getMessageClass(), bytes);
            Object message = deserializationCache.get(deserializationKey);
            if (message != null) {
                // pre cached
                return (T) message;
            }
        }
        final T message = deserializeWithoutCache(deserializer, bytes);

        // check if the message is immutable
        final Message messageAnnotation = message.getClass().getAnnotation(Message.class);
        if (messageAnnotation != null && messageAnnotation.immutable()) {
            // optimize serialization as well
            Map<Object, ByteBuffer> serializationCache = serializationCacheEnabled
                ? SerializationContext.serializationCache.get()
                : null;
            if (serializationCache != null) {
                serializationCache.put(message, bytes);
            }
            if (deserializationCache != null) {
                // this should speed up subsequent deserializations of the same bytes
                deserializationCache.put(
                    new DeserializationKey(
                        deserializer.getMessageClass(),
                        bytes
                    ),
                    message
                );
            }
        }
        return message;
    }

    public static ByteBuffer serialize(final MessageSerializer serializer, final Object message) throws IOException {

        if (serializationCacheEnabled || deserializationCacheEnabled) {
            return serializeWithCache(serializer, message);
        } else {
            return serializer.serialize(message);
        }

    }

    private static ByteBuffer serializeWithCache(final MessageSerializer serializer, final Object message) throws IOException {
        // check if the message is immutable
        final Message messageAnnotation = message.getClass().getAnnotation(Message.class);
        if (messageAnnotation != null && messageAnnotation.immutable()) {
            Map<Object, ByteBuffer> serializationCache = serializationCacheEnabled
                ? SerializationContext.serializationCache.get()
                : null;
            ByteBuffer cachedBuffer = serializationCache != null
                ? serializationCache.get(message)
                : null;
            if (cachedBuffer != null) {
                // we have a cached version
                // return a copy
                return cachedBuffer;
            } else {
                // didn't find a buffer, but the message is immutable
                ByteBuffer serializedBuffer = serializer.serialize(message);
                // store it
                if (serializationCache != null) {
                    serializationCache.put(message, serializedBuffer);
                }
                Map<DeserializationKey, Object> deserializationCache = deserializationCacheEnabled
                    ? SerializationContext.deserializationCache.get()
                    : null;
                if (deserializationCache != null) {
                    deserializationCache.put(new DeserializationKey(
                        message.getClass(),
                        serializedBuffer
                    ), message);
                }
                return serializedBuffer;
            }
        }
        // message is not immutable, just serialize it
        return serializer.serialize(message);
    }

    private static final class DeserializationKey {
        private Class<?> objectClass;
        private ByteBuffer bytes;

        public DeserializationKey(Class<?> objectClass, ByteBuffer bytes) {
            this.objectClass = objectClass;
            this.bytes = bytes;
        }

        public void setObjectClass(Class<?> objectClass) {
            this.objectClass = objectClass;
        }

        public void setBytes(ByteBuffer bytes) {
            this.bytes = bytes;
        }

        /**
         * Convenience method to populate the object with new values
         */
        public void populate(Class<?> objectClass, ByteBuffer bytes) {
            this.objectClass = objectClass;
            this.bytes = bytes;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            DeserializationKey that = (DeserializationKey) o;

            return objectClass.equals(that.objectClass) && bytes.equals(that.bytes);

        }

        @Override
        public int hashCode() {
            int result = objectClass.hashCode();
            result = 31 * result + bytes.hashCode();
            return result;
        }
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
