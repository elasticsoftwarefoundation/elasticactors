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

package org.elasterix.elasticactors.cluster;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.elasterix.elasticactors.*;
import org.elasterix.elasticactors.messaging.*;
import org.elasterix.elasticactors.serialization.MessageSerializer;
import org.elasterix.elasticactors.state.PersistentActor;
import org.elasterix.elasticactors.util.concurrent.ActorSystemShardExecutor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Configurable;
import org.springframework.beans.factory.annotation.Qualifier;

import java.util.concurrent.locks.ReadWriteLock;

/**
 * @author Joost van de Wijgerd
 */
@Configurable
public class LocalActorShard implements ActorShard, MessageHandler {
    private final ActorSystem actorSystem;
    private final PhysicalNode localNode;
    private final ShardKey shardKey;
    private final MessageQueueFactory messageQueueFactory;
    private MessageQueue messageQueue;
    private ActorSystemShardExecutor actorExecutor;
    private Cache<ActorRef,PersistentActor> actorCache;

    public LocalActorShard(PhysicalNode node, ActorSystem actorSystem, int shard, MessageQueueFactory messageQueueFactory) {
        this.actorSystem = actorSystem;
        this.localNode = node;
        this.shardKey = new ShardKey(actorSystem.getName(), shard);
        this.messageQueueFactory = messageQueueFactory;
    }

    @Override
    public void init() throws Exception {
        //@todo: this cache needs to be parameterized
        this.actorCache = CacheBuilder.newBuilder().build();
        this.messageQueue = messageQueueFactory.create(shardKey.toString(), this);
    }

    @Override
    public void destroy() {
        // release all resources
        this.messageQueue.destroy();
    }

    @Override
    public PhysicalNode getOwningNode() {
        return localNode;
    }

    @Override
    public ShardKey getKey() {
        return shardKey;
    }

    public void sendMessage(ActorRef from, ActorRef to, Object message) throws Exception {
        MessageSerializer messageSerializer = actorSystem.getSerializer(message.getClass());
        messageQueue.offer(new InternalMessageImpl(from, to, messageSerializer.serialize(message), message.getClass().getName()));
    }

    @Override
    public void handleMessage(InternalMessage message) {
        ActorRef receiverRef = message.getReceiver();
        if(receiverRef != null) {
            // find actor class behind receiver ActorRef
            ElasticActor actorInstance = actorSystem.getActorInstance(message.getReceiver());
            // execute on it's own thread
            actorExecutor.execute(new HandleMessageTask(actorSystem,actorInstance,message,actorCache.getIfPresent(message.getReceiver())));
        } else {
            // the message is intended for the shard, this means it's about creating or stopping an actor
            // we will handle this on the messaging thread as it's all serialized and we can change the

        }
    }

    @Autowired
    public void setActorExecutor(ActorSystemShardExecutor actorExecutor) {
        this.actorExecutor = actorExecutor;
    }
}

