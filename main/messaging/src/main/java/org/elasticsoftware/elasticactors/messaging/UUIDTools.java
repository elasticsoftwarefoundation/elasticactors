/*
 *   Copyright 2013 - 2019 The Original Authors
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.elasticsoftware.elasticactors.messaging;

import com.fasterxml.uuid.Generators;
import com.fasterxml.uuid.impl.TimeBasedGenerator;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.UUID;

/**
 * @author Joost van de Wijgerd
 */
public final class UUIDTools {
    private static final TimeBasedGenerator generator = Generators.timeBasedGenerator();
    private static final int UUID_SIZE = Long.BYTES * 2;

    private UUIDTools() {}

    /**
     * Convert a UUID to its byte[] representation. Will return a byte[] of length 16
     *
     * @param uuid the UUID to convert
     * @return the bytes of the UUID
     */
    public static byte[] toByteArray(UUID uuid) {
        final long msb = uuid.getMostSignificantBits();
        final long lsb = uuid.getLeastSignificantBits();
        byte[] buffer = new byte[UUID_SIZE];

        for (int i = 0; i < 8; i++) {
            buffer[i] = (byte) (msb >>> (8 * (7 - i)));
        }
        for (int i = 8; i < 16; i++) {
            buffer[i] = (byte) (lsb >>> (8 * (15 - i)));
        }

        return buffer;
    }

    public static ByteBuffer toByteBuffer(UUID uuid) {
        final long msb = uuid.getMostSignificantBits();
        final long lsb = uuid.getLeastSignificantBits();
        ByteBuffer buffer = ByteBuffer.allocate(UUID_SIZE);
        buffer.putLong(msb);
        buffer.putLong(lsb);
        buffer.rewind();
        return buffer;
    }

    public static UUID toUUID(ByteBuffer uuid) {
        if (uuid == null || uuid.remaining() != UUID_SIZE) {
            throw new IllegalArgumentException(String.format(
                "UUID byte array must contain exactly %d bytes",
                UUID_SIZE
            ));
        }
        if (uuid.hasArray()) {
            return toUUIDInternal(uuid.array());
        } else {
            int position = uuid.position();
            final long msb = uuid.getLong();
            final long lsb = uuid.getLong();
            uuid.position(position);
            return new UUID(msb, lsb);
        }
    }

    public static UUID toUUID(byte[] uuid) {
        if (uuid == null || uuid.length != UUID_SIZE) {
            throw new IllegalArgumentException(String.format(
                "UUID byte array must contain exactly %d bytes",
                UUID_SIZE
            ));
        }
        return toUUIDInternal(uuid);
    }

    private static UUID toUUIDInternal(byte[] uuid) {
        long msb = 0;
        long lsb = 0;
        for (int i = 0; i < 8; i++) {
            msb = (msb << 8) | (uuid[i] & 0xff);
        }
        for (int i = 8; i < 16; i++) {
            lsb = (lsb << 8) | (uuid[i] & 0xff);
        }

        return new UUID(msb, lsb);
    }

    public static UUID createTimeBasedUUID() {
        return generator.generate();
    }

    public static UUID createRandomUUID() {
        return UUID.randomUUID();
    }

    public static UUID fromString(String uuid) {
        return UUID.fromString(uuid);
    }

    public static long toUnixTimestamp(UUID uuid) {
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
