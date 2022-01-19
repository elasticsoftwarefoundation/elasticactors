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

package org.elasticsoftware.elasticactors.runtime;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.elasticsoftware.elasticactors.RemoteActorSystemConfiguration;

/**
 * @author Joost van de Wijgerd
 */
public final class DefaultRemoteConfiguration implements RemoteActorSystemConfiguration {
    private final String clusterName;
    private final String name;
    private final int numberOfShards;
    private final int queuesPerShard;
    private final int shardHashSeed;
    private final int multiQueueHashSeed;

    @JsonCreator
    public DefaultRemoteConfiguration(
        @JsonProperty("clusterName") String clusterName,
        @JsonProperty("name") String name,
        @JsonProperty("shards") int numberOfShards,
        @JsonProperty("queuesPerShard") Integer queuesPerShard,
        @JsonProperty("shardHashSeed") Integer shardHashSeed,
        @JsonProperty("multiQueueHashSeed") Integer multiQueueHashSeed)
    {
        this.clusterName = clusterName;
        this.name = name;
        this.numberOfShards = numberOfShards;
        this.queuesPerShard = queuesPerShard != null ? queuesPerShard : 1;
        this.shardHashSeed = shardHashSeed != null ? shardHashSeed : 0;
        this.multiQueueHashSeed = multiQueueHashSeed != null ? multiQueueHashSeed : 53;
    }

    @JsonProperty("name")
    @Override
    public String getName() {
        return name;
    }

    @JsonProperty("shards")
    @Override
    public int getNumberOfShards() {
        return numberOfShards;
    }

    @JsonProperty("queuesPerShard")
    @Override
    public int getQueuesPerShard() {
        return queuesPerShard;
    }

    @Override
    public String getClusterName() {
        return clusterName;
    }

    @JsonProperty("shardHashSeed")
    @Override
    public int getShardHashSeed() {
        return shardHashSeed;
    }

    @JsonProperty("multiQueueHashSeed")
    @Override
    public int getMultiQueueHashSeed() {
        return multiQueueHashSeed;
    }
}
