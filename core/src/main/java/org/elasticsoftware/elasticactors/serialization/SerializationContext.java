/*
 * Copyright 2013 - 2016 The Original Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
    private static final ThreadLocal<EvictingMap<DeserializationKey,Object>> deserializationCache = new ThreadLocal<>();
    // keypool to avoid too much garbage being generated
    private static final ThreadLocal<DeserializationKey> keyPool = new ThreadLocal<DeserializationKey>() {
        @Override
        protected DeserializationKey initialValue() {
            return new DeserializationKey(null,null);
        }
    };
    private static final boolean deserializationCacheEnabled = Boolean.valueOf(System.getProperty("ea.deserializationCache.enabled", "false"));
    private static final boolean serializationCacheEnabled = Boolean.valueOf(System.getProperty("ea.serializationCache.enabled", "false"));

    private SerializationContext() {}

    public static void initialize() {
        // only create once per thread
        if(serializationCacheEnabled && serializationCache.get() == null) {
            serializationCache.set(new IdentityHashMap<Object, ByteBuffer>());
        }
        if(deserializationCacheEnabled && deserializationCache.get() == null) {
            deserializationCache.set(new EvictingMap<DeserializationKey, Object>());
        }
    }

    public static void reset() {
        if(serializationCache.get() != null) {
            serializationCache.get().clear();
        }
    }

    public static <T> T deserialize(final MessageDeserializer<T> deserializer,final ByteBuffer bytes) throws IOException {
        final EvictingMap<DeserializationKey, Object> deserializationCache = (deserializationCacheEnabled) ? SerializationContext.deserializationCache.get() : null;

        // see if we already have the deserialized version cached
        if(deserializationCache != null) {
            Object message = deserializationCache.get(keyPool.get().populate(deserializer.getMessageClass(), bytes));
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
            // optimize serialization as well
            serializationCache.get().put(message,bytes.asReadOnlyBuffer());
            if(deserializationCache != null) {
                // this should speed up subsequent deserializations of the same bytes
                deserializationCache.put(new DeserializationKey(deserializer.getMessageClass(),bytes.asReadOnlyBuffer()), message);
            }
        }
        return message;
    }

    public static ByteBuffer serialize(final MessageSerializer serializer, final Object message) throws IOException {
        // check if the message is immutable
        //final Message messsageAnnotation = message.getClass().getAnnotation(Message.class);
        final Message immutableAnnotation = message.getClass().getAnnotation(Message.class);
        if(immutableAnnotation != null && immutableAnnotation.immutable() && serializationCache.get() != null) {
            final EvictingMap<DeserializationKey, Object> deserializationCache = (deserializationCacheEnabled) ? SerializationContext.deserializationCache.get() : null;
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
                    deserializationCache.put(new DeserializationKey(message.getClass(),serializedBuffer.asReadOnlyBuffer()), message);
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
         * Convenient method to populate the object with new values
         *
         * @param objectClass
         * @param bytes
         * @return
         */
        public DeserializationKey populate(Class<?> objectClass, ByteBuffer bytes) {
            this.objectClass = objectClass;
            this.bytes = bytes;
            return this;
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
