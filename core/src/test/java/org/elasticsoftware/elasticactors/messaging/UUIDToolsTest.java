/*
 * Copyright 2013 - 2015 The Original Authors
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

package org.elasticsoftware.elasticactors.messaging;

import org.testng.annotations.Test;

import java.util.UUID;

import static org.testng.Assert.assertEquals;

/**
 * @author Joost van de Wijgerd
 */
public class UUIDToolsTest {
    @Test
    public void testTimeBasedUUID() {
        UUID uuid = UUIDTools.createTimeBasedUUID();
        assertEquals(uuid.version(),1);
        byte[] uuidBytes = UUIDTools.toByteArray(uuid);
        UUID convertedUuid = UUIDTools.toUUID(uuidBytes);
        assertEquals(convertedUuid,uuid);
    }

    @Test
    public void testPerformance() {
        // warm it up
        for (int i = 0; i < 1000; i++) {
            UUIDTools.createTimeBasedUUID();
        }
        // now time
        long startTime = System.nanoTime();
        for (int i = 0; i < 10000; i++) {
            UUIDTools.createTimeBasedUUID();
        }
        long runningTime = System.nanoTime() - startTime;
        //
        System.out.println("New implementation took "+runningTime/10000+" nanos per invocation");
    }
}
