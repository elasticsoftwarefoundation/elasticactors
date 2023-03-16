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

package org.elasticsoftware.elasticactors.util;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.ToIntFunction;

public final class ByteBufferUtils {

    private final static Function<ByteBuffer, CharBuffer> utf8Decoder =
        StandardCharsets.UTF_8::decode;

    private ByteBufferUtils() {
    }

    public static byte[] toByteArrayAndReset(ByteBuffer buffer) {
        if (buffer.hasArray()) {
            return buffer.array();
        } else {
            return doAndReset(buffer, ByteBufferUtils::internalToByteArray);
        }
    }

    public static byte[] toByteArray(ByteBuffer buffer) {
        if (buffer.hasArray()) {
            return buffer.array();
        } else {
            return internalToByteArray(buffer);
        }
    }

    private static byte[] internalToByteArray(ByteBuffer buffer) {
        byte[] messageBytes = new byte[buffer.remaining()];
        buffer.get(messageBytes);
        return messageBytes;
    }

    public static String decodeUtf8String(ByteBuffer buffer) {
        if (buffer.hasArray()) {
            return new String(buffer.array(), StandardCharsets.UTF_8);
        } else {
            return doAndReset(buffer, utf8Decoder).toString();
        }
    }

    public static <T, E extends Throwable> T throwingApplyAndReset(
        ByteBuffer buffer,
        ThrowingByteBufferFunction<T, E> function) throws E
    {
        int position = buffer.position();
        try {
            return function.apply(buffer);
        } finally {
            buffer.position(position);
        }
    }

    public static <E extends Throwable> boolean throwingTestAndReset(
        ByteBuffer buffer,
        ThrowingByteBufferPredicate<E> predicate) throws E
    {
        int position = buffer.position();
        try {
            return predicate.test(buffer);
        } finally {
            buffer.position(position);
        }
    }

    public static int toIntAndReset(ByteBuffer buffer, ToIntFunction<ByteBuffer> function) {
        int position = buffer.position();
        try {
            return function.applyAsInt(buffer);
        } finally {
            buffer.position(position);
        }
    }

    public static <T> T doAndReset(ByteBuffer buffer, Function<ByteBuffer, T> function) {
        int position = buffer.position();
        try {
            return function.apply(buffer);
        } finally {
            buffer.position(position);
        }
    }

    public static boolean testAndReset(ByteBuffer buffer, Predicate<ByteBuffer> predicate) {
        int position = buffer.position();
        try {
            return predicate.test(buffer);
        } finally {
            buffer.position(position);
        }
    }

    @FunctionalInterface
    public interface ThrowingByteBufferFunction<T, E extends Throwable> {

        T apply(ByteBuffer buffer) throws E;
    }

    @FunctionalInterface
    public interface ThrowingByteBufferPredicate<E extends Throwable> {

        boolean test(ByteBuffer buffer) throws E;
    }

}
