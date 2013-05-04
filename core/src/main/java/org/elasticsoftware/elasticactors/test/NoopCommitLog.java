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

package org.elasticsoftware.elasticactors.test;

import org.elasticsoftware.elasticactors.messaging.CommitLog;

import java.util.Collections;
import java.util.List;
import java.util.UUID;

/**
 * @author Joost van de Wijgerd
 */
public class NoopCommitLog implements CommitLog {
    @Override
    public void append(String segment, UUID messageId, byte[] data) {
        // do nothing
    }

    @Override
    public void delete(String segment, UUID messageId) {
        // do nothing
    }

    @Override
    public List<CommitLogEntry> replay(String segment) {
        return Collections.emptyList();
    }
}
