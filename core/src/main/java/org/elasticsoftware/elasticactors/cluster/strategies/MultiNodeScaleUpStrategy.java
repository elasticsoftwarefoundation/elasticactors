/*
 * Copyright 2013 - 2017 The Original Authors
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

package org.elasticsoftware.elasticactors.cluster.strategies;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsoftware.elasticactors.ActorShard;
import org.elasticsoftware.elasticactors.PhysicalNode;
import org.elasticsoftware.elasticactors.ShardKey;
import org.elasticsoftware.elasticactors.cluster.ShardDistributionStrategy;
import org.elasticsoftware.elasticactors.cluster.messaging.ShardReleasedMessage;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;

/**
 * @author Joost van de Wijgerd
 */
public abstract class MultiNodeScaleUpStrategy implements ShardDistributionStrategy {
    protected final Logger logger = LogManager.getLogger(this.getClass());
    private final LinkedBlockingQueue<ShardReleasedMessage> shardReleasedMessages;
    private final Map<ShardKey,ActorShard> registeredShards = new HashMap<>();

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
            logger.info(format("Waiting maximum of %d %s for %d Shards to be released by Remote Nodes",maxWaitTime,unit.name(),registeredShards.size()));
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
                        logger.info(format("Initializing LocalShard %s",shardKey.toString()));
                        localShard.init();
                    } else {
                        logger.error(format("IMPORTANT: Got a ShardReleasedMessage for an unregistered shard [%s], ElasticActors cluster is unstable. Please check all nodes",shardKey));
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
            logger.info(format("Going ahead with initializing %d Local Shards",registeredShards.size()));
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
