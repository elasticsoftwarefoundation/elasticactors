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

package org.elasticsoftware.elasticactors.messaging.journal;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import org.elasticsoftware.elasticactors.messaging.CommitLog;
import org.elasticsoftware.elasticactors.messaging.UUIDTools;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

import java.util.List;
import java.util.UUID;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * @author Joost van de Wijgerd
 */
@ContextConfiguration(locations = {"classpath:cluster-beans.xml"})
public class CassandraCommitLogTest extends AbstractTestNGSpringContextTests {
    @Autowired
    private CassandraCommitLog commitLog;

    @Test(enabled = false)
    public void testAppendReplayAndDelete() {
        UUID firstId = UUIDTools.createTimeBasedUUID();
        UUID secondId = UUIDTools.createTimeBasedUUID();
        UUID thirdId = UUIDTools.createTimeBasedUUID();
        commitLog.append("IntegrationTest:0", firstId, "This is test message 1".getBytes(Charsets.UTF_8));
        commitLog.append("IntegrationTest:0", secondId, "This is test message 2".getBytes(Charsets.UTF_8));
        commitLog.append("IntegrationTest:0", thirdId, "This is test message 3".getBytes(Charsets.UTF_8));

        // fetch them all
        List<CommitLog.CommitLogEntry> entries = commitLog.replay("IntegrationTest:0");
        assertTrue(entries.size() >= 3);
        // our messages should be the last 3 (or the first three inverted)
        entries = Lists.reverse(entries);
        assertEquals(entries.get(2).getMessageId(), firstId);
        assertEquals(entries.get(1).getMessageId(),secondId);
        assertEquals(entries.get(0).getMessageId(),thirdId);

        // delete them all
        for (CommitLog.CommitLogEntry entry : entries) {
            commitLog.delete("IntegrationTest:0",entry.getMessageId());
        }

        // now we should have zero left
        entries = commitLog.replay("IntegrationTest:0");
        assertTrue(entries.isEmpty());
    }


}
