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

package org.elasticsoftware.elasticactors.cluster.strategies;

import org.elasticsoftware.elasticactors.ActorShard;
import org.elasticsoftware.elasticactors.PhysicalNode;
import org.elasticsoftware.elasticactors.cluster.ShardDistributionStrategy;

import java.util.concurrent.TimeUnit;

/**
 * @author Joost van de Wijgerd
 */
public final class SingleNodeScaleUpStrategy implements ShardDistributionStrategy {
    @Override
    public void signalRelease(ActorShard localShard, PhysicalNode nextOwner) {
        // won't happen. as we are the single node in the cluster
    }

    @Override
    public void registerWaitForRelease(ActorShard localShard, PhysicalNode currentOwner) throws Exception {
        // no need to wait, run init immediately
        localShard.init();
    }

    @Override
    public boolean waitForReleasedShards(long waitTime, TimeUnit unit) {
        return true;
    }
}
