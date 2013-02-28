/*
 * Copyright 2013 Joost van de Wijgerd
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

package org.elasterix.elasticactors.messaging;

import com.eaio.uuid.UUIDGen;

import java.util.Comparator;
import java.util.UUID;

/**
 * @author Joost van de Wijgerd
 */
public final class UUIDTools {

    /**
     * Convert a UUID to it's byte[] representation. Will return a byte[] of length 16
     *
     * @param uuid the UUID to convert
     * @return the bytes of the UUID
     */
    public static byte[] toByteArray(UUID uuid) {
        long msb = uuid.getMostSignificantBits();
        long lsb = uuid.getLeastSignificantBits();
        byte[] buffer = new byte[16];

        for (int i = 0; i < 8; i++) {
            buffer[i] = (byte) (msb >>> 8 * (7 - i));
        }
        for (int i = 8; i < 16; i++) {
            buffer[i] = (byte) (lsb >>> 8 * (7 - i));
        }

        return buffer;
    }

    public static java.util.UUID toUUID(byte[] uuid) {
        if (uuid == null || uuid.length != 16) {
            throw new IllegalArgumentException("UUID byte array must contain exactly 16 bytes");
        }
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
        return java.util.UUID.fromString(new com.eaio.uuid.UUID(UUIDGen.newTime(), com.eaio.uuid.UUIDGen.getClockSeqAndNode()).toString());
    }

    public static UUID createRandomUUID() {
        return UUID.randomUUID();
    }

    public static UUID fromString(String uuid) {
        return UUID.fromString(uuid);
    }

    public static long toUnixTimestamp(UUID uuid) {
        long t = uuid.timestamp();
        // 0x01b21dd213814000 is the number of 100-ns intervals between the
        // UUID epoch 1582-10-15 00:00:00 and the Unix epoch 1970-01-01 00:00:00.
        t = t - 0x01b21dd213814000L;
        t = (long) (t / 1e4); //Convert to ms
        return t;
    }

    private static Comparator<UUID> timeBasedComparator = null;

    public static Comparator<UUID> getTimeBasedComparator() {
        if (timeBasedComparator == null) {
            timeBasedComparator = new Comparator<UUID>() {

                @Override
                public int compare(UUID o1, UUID o2) {
                    long time1 = o1.timestamp();
                    long time2 = o2.timestamp();
                    if (time1 == time2) return 0;
                    //In case time1 and time2 are both positive we only do (time1 < time2)
                    //In case time1 is negative and time2 not, we reverse the comparison with (^time1 < 0)
                    //In case time1 is negative and time2 is too, we reverse the comparison twice.
                    boolean smaller = (time1 < time2) ^ (time1 < 0) ^ (time2 < 0);
                    return smaller ? -1 : 1;
                }
            };
        }
        return timeBasedComparator;
    }


}
