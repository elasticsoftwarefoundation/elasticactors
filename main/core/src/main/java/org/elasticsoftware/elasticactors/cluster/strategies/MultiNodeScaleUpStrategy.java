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

package org.elasticsoftware.elasticactors.cluster.strategies;

import org.elasticsoftware.elasticactors.ActorShard;
import org.elasticsoftware.elasticactors.PhysicalNode;
import org.elasticsoftware.elasticactors.ShardKey;
import org.elasticsoftware.elasticactors.cluster.ShardDistributionStrategy;
import org.elasticsoftware.elasticactors.cluster.messaging.ShardReleasedMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * @author Joost van de Wijgerd
 */
public abstract class MultiNodeScaleUpStrategy implements ShardDistributionStrategy {
    protected final Logger logger = LoggerFactory.getLogger(this.getClass());
    private final LinkedBlockingQueue<ShardReleasedMessage> shardReleasedMessages;
    private final ConcurrentMap<ShardKey,ActorShard> registeredShards = new ConcurrentHashMap<>();

    public MultiNodeScaleUpStrategy(LinkedBlockingQueue<ShardReleasedMessage> shardReleasedMessages) {
        this.shardReleasedMessages = shardReleasedMessages;
    }

    @Override
    public final void registerWaitForRelease(ActorShard localShard, PhysicalNode currentOwner) throws Exception {
        this.registeredShards.put(localShard.getKey(),localShard);
    }

    @Override
    public final boolean waitForReleasedShards(final long maxWaitTime,final TimeUnit unit) {
        final long startTime = System.currentTimeMillis();
        final long maxWaitTimeMillis = startTime + TimeUnit.MILLISECONDS.convert(maxWaitTime,unit);
        long waitTime = TimeUnit.MILLISECONDS.convert(maxWaitTime,unit);
        if(!registeredShards.isEmpty()) {
            logger.info("Waiting maximum of {} {} for {} Shards to be released by Remote Nodes",maxWaitTime,unit.name(),registeredShards.size());
        }
        while(!registeredShards.isEmpty() && maxWaitTimeMillis >= System.currentTimeMillis()) {
            try {
                // run over the blocking queue
                ShardReleasedMessage shardReleasedMessage = shardReleasedMessages.poll(waitTime,TimeUnit.MILLISECONDS);
                if(shardReleasedMessage != null) {
                    ShardKey shardKey = new ShardKey(shardReleasedMessage.getActorSystem(),shardReleasedMessage.getShardId());
                    // handle the shard
                    ActorShard localShard = registeredShards.remove(shardKey);
                    if(localShard != null) {
                        logger.info("Initializing LocalShard {}",shardKey);
                        localShard.init();
                    } else {
                        logger.error("IMPORTANT: Got a ShardReleasedMessage for an unregistered shard [{}], ElasticActors cluster is unstable. Please check all nodes",shardKey);
                    }
                }
                // reset the waitTime interval
                waitTime = maxWaitTimeMillis - System.currentTimeMillis();
            } catch(InterruptedException e) {
                //ignore
            } catch(Exception e) {
                logger.error("IMPORTANT: Exception on initializing LocalShard, ElasticActors cluster is unstable. Please check all nodes",e);
                return false;
            }
        }

        if(!registeredShards.isEmpty()) {
            logger.warn("Timed out while waiting for Shards to be released");
            logger.info("Going ahead with initializing {} Local Shards",registeredShards.size());
            for (ActorShard actorShard : registeredShards.values()) {
                try {
                    actorShard.init();
                } catch (Exception e) {
                    logger.error("IMPORTANT: Exception on initializing LocalShard, ElasticActors cluster is unstable. Please check all nodes",e);
                    return false;
                }
            }
            registeredShards.clear();
        } else {
            logger.info("Finished waiting for Shards to be released");
        }

        return true;
    }
}
