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
import org.elasterix.elasticactors.util.concurrent.ActorSystemShardExecutor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Configurable;
import org.springframework.beans.factory.annotation.Qualifier;

/**
 * @author Joost van de Wijgerd
 */
@Configurable
public class LocalActorSystemShard implements ActorSystemShard, MessageHandler {
    private final ActorSystem actorSystem;
    private final PhysicalNode localNode;
    private final ShardKey shardKey;
    private MessageQueue messageQueue;
    private MessageQueueFactory messageQueueFactory;
    private ActorSystemShardExecutor actorExecutor;
    private Cache<String,ActorState> actorStateCache;

    public LocalActorSystemShard(PhysicalNode node, ActorSystem actorSystem, int vNodeKey, MessageQueue remoteMessageQueue) {
        this.actorSystem = actorSystem;
        this.localNode = node;
        this.messageQueue = remoteMessageQueue;
        this.shardKey = new ShardKey(actorSystem.getName(), vNodeKey);
    }

    public void init() {
        this.messageQueue = messageQueueFactory.create(shardKey.toString(),this);
        //@todo: this cache needs to be parameterized
        this.actorStateCache = CacheBuilder.newBuilder().build();
    }

    public void destroy() {
        // release all recources
        this.messageQueue.destroy();
    }

    @Override
    public ShardKey getKey() {
        return shardKey;
    }

    public void sendMessage(ActorRef from, ActorRef to, Object message) throws Exception {
        MessageSerializer<Object> messageSerializer = actorSystem.getSerializer(message.getClass());
        messageQueue.offer(new InternalMessageImpl(from, to, messageSerializer.serialize(message), message.getClass()));
    }

    @Override
    public void handleMessage(InternalMessage message) {
        // find actor class behind receiver ActorRef
        ElasticActor actorInstance = actorSystem.getActorInstance(message.getReceiver());
        // execute on it's own thread
        actorExecutor.execute(new HandleMessageTask(actorSystem,actorInstance,message,actorStateCache));
    }

    @Autowired
    public void setMessageQueueFactory(@Qualifier("localMessageQueueFactory") MessageQueueFactory messageQueueFactory) {
        this.messageQueueFactory = messageQueueFactory;
    }

    @Autowired
    public void setActorExecutor(ActorSystemShardExecutor actorExecutor) {
        this.actorExecutor = actorExecutor;
    }
}

