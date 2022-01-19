/*
 *   Copyright 2013 - 2022 The Original Authors
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

package org.elasticsoftware.elasticactors.cluster;

import org.elasticsoftware.elasticactors.ActorShard;
import org.elasticsoftware.elasticactors.PhysicalNode;

import java.util.concurrent.TimeUnit;

/**
 * @author Joost van de Wijgerd
 */
public interface ShardDistributionStrategy {
    /**
     * Signal to the cluster that a Local ActorShard has been given up
     *
     * @param localShard
     * @param nextOwner
     */
    public void signalRelease(ActorShard localShard,PhysicalNode nextOwner) throws Exception;
    /**
     * Wait for signal from the current shard owner, when the signal comes in {@link org.elasticsoftware.elasticactors.ActorContainer#init()}
     * should be called within the implementation
     *
     * @param localShard
     * @param currentOwner
     */
    public void registerWaitForRelease(ActorShard localShard,PhysicalNode currentOwner) throws Exception;
    /**
     *  Block until all Shard that were registered to wait for release are handled, or the specified
     *  waitTime has run out.
     *
     * @param waitTime
     * @param unit
     */
    public boolean waitForReleasedShards(long waitTime,TimeUnit unit);
}
