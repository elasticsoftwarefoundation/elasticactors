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

package org.elasticsoftware.elasticactors.cluster.scheduler;

import org.elasticsoftware.elasticactors.ShardKey;
import org.elasticsoftware.elasticactors.scheduler.Scheduler;

/**
 * @author Joost van de Wijgerd
 */
public interface SchedulerService extends Scheduler,InternalScheduler {
    /**
     * Register a local shard with the SchedulerService. The implementation of the service should
     * load the proper resources for the shard
     *
     * @param shardKey
     */
    public void registerShard(ShardKey shardKey);

    /**
     * Unregister a previously registered shard, release all allocated resources as another node is
     * the new owner of this shard
     *
     * @param shardKey
     */
    public void unregisterShard(ShardKey shardKey);
}
