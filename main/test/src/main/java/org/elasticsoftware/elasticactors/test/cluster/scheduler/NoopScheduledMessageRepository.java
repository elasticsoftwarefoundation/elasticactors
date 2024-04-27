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

package org.elasticsoftware.elasticactors.test.cluster.scheduler;

import org.elasticsoftware.elasticactors.ShardKey;
import org.elasticsoftware.elasticactors.cluster.scheduler.ScheduledMessage;
import org.elasticsoftware.elasticactors.cluster.scheduler.ScheduledMessageKey;
import org.elasticsoftware.elasticactors.cluster.scheduler.ScheduledMessageRepository;

import java.util.Collections;
import java.util.List;

/**
 * @author Joost van de Wijgerd
 */
public final class NoopScheduledMessageRepository implements ScheduledMessageRepository {
    @Override
    public void create(ShardKey shardKey, ScheduledMessage scheduledMessage) {
        // do nothing
    }

    @Override
    public void delete(ShardKey shardKey, ScheduledMessageKey scheduledMessage) {
        // do nothing
    }

    @Override
    public List<ScheduledMessage> getAll(ShardKey shardKey) {
        return Collections.emptyList();
    }
}
