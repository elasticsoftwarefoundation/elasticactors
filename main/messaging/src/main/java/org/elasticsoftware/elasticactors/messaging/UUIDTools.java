/*
 * Copyright 2013 - 2025 The Original Authors
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

package org.elasticsoftware.elasticactors.messaging;

import com.eatthepath.uuid.FastUUID;
import com.fasterxml.uuid.Generators;
import com.fasterxml.uuid.impl.TimeBasedGenerator;
import com.fasterxml.uuid.impl.UUIDUtil;
import com.google.protobuf.ByteString;
import org.elasticsoftware.elasticactors.util.ByteBufferUtils;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.UUID;

/**
 * @author Joost van de Wijgerd
 */
public final class UUIDTools {
    private static final TimeBasedGenerator generator = Generators.timeBasedGenerator();

    private UUIDTools() {}

    /**
     * Convert a UUID to its byte[] representation. Will return a byte[] of length 16
     *
     * @param uuid the UUID to convert
     * @return the bytes of the UUID
     */
    @Nonnull
    public static byte[] toByteArray(@Nonnull UUID uuid) {
        return UUIDUtil.asByteArray(uuid);
    }

    @Nonnull
    public static ByteBuffer toByteBuffer(@Nonnull UUID uuid) {
        return ByteBuffer.wrap(toByteArray(uuid));
    }

    @Nonnull
    public static ByteString toByteString(@Nonnull UUID uuid) {
        return ByteString.copyFrom(toByteArray(uuid));
    }

    @Nonnull
    public static UUID fromByteArray(@Nonnull byte[] uuid) {
        return fromByteArrayInternal(uuid);
    }

    @Nonnull
    public static UUID fromByteBuffer(@Nonnull ByteBuffer uuid) {
        if (uuid.hasArray()) {
            return fromByteArrayInternal(uuid.array());
        } else {
            if (uuid.remaining() != 16) {
                throw new IllegalArgumentException(
                    "UUID byte buffer must have exactly 16 bytes remaining");
            }
            return ByteBufferUtils.doAndReset(uuid, UUIDTools::fromByteBufferInternal);
        }
    }

    @Nonnull
    private static UUID fromByteBufferInternal(@Nonnull ByteBuffer uuid) {
        final long msb = uuid.getLong();
        final long lsb = uuid.getLong();
        return new UUID(msb, lsb);
    }

    @Nonnull
    public static UUID fromByteString(@Nonnull ByteString uuid) {
        return fromByteArrayInternal(uuid.toByteArray());
    }

    @Nonnull
    private static UUID fromByteArrayInternal(@Nonnull byte[] uuid) {
        return UUIDUtil.uuid(uuid);
    }

    @Nonnull
    public static UUID createTimeBasedUUID() {
        return generator.generate();
    }

    @Nonnull
    public static String createTimeBasedUUIDString() {
        return toString(generator.generate());
    }

    @Nonnull
    public static UUID createRandomUUID() {
        return UUID.randomUUID();
    }

    @Nonnull
    public static String createRandomUUIDString() {
        return toString(UUID.randomUUID());
    }

    @Nonnull
    public static UUID fromString(@Nonnull String uuid) {
        return FastUUID.parseUUID(uuid);
    }

    @Nonnull
    public static String toString(@Nonnull UUID uuid) {
        return FastUUID.toString(uuid);
    }

    public static long toUnixTimestamp(@Nonnull UUID uuid) {
        // 0x01b21dd213814000 is the number of 100-ns intervals between the
        // UUID epoch 1582-10-15 00:00:00 and the Unix epoch 1970-01-01 00:00:00.
        return (uuid.timestamp() - 0x01b21dd213814000L) / 10_000L;
    }

    private static Comparator<UUID> timeBasedComparator = null;

    public static Comparator<UUID> getTimeBasedComparator() {
        if (timeBasedComparator == null) {
            timeBasedComparator = (o1, o2) -> {
                long time1 = o1.timestamp();
                long time2 = o2.timestamp();
                if (time1 == time2) return 0;
                //In case time1 and time2 are both positive we only do (time1 < time2)
                //In case time1 is negative and time2 not, we reverse the comparison with (^time1 < 0)
                //In case time1 is negative and time2 is too, we reverse the comparison twice.
                boolean smaller = (time1 < time2) ^ (time1 < 0) ^ (time2 < 0);
                return smaller ? -1 : 1;
            };
        }
        return timeBasedComparator;
    }


}
