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

package org.elasticsoftware.elasticactors.redis.state;

import org.elasticsoftware.elasticactors.ShardKey;
import org.elasticsoftware.elasticactors.messaging.InternalMessage;
import org.elasticsoftware.elasticactors.messaging.MessageHandlerEventListener;
import org.elasticsoftware.elasticactors.serialization.Deserializer;
import org.elasticsoftware.elasticactors.serialization.Serializer;
import org.elasticsoftware.elasticactors.state.PersistentActor;
import org.elasticsoftware.elasticactors.state.PersistentActorRepository;
import org.springframework.data.redis.core.RedisTemplate;

import java.io.IOException;

/**
 * @author Joost van de Wijgerd
 */
public class RedisPersistentActorRepository implements PersistentActorRepository {
    private final RedisTemplate<String,byte[]> redisTemplate;
    private final Deserializer<byte[],PersistentActor> deserializer;
    private final Serializer<PersistentActor,byte[]> serializer;

    public RedisPersistentActorRepository(RedisTemplate<String, byte[]> redisTemplate, Deserializer<byte[], PersistentActor> deserializer, Serializer<PersistentActor, byte[]> serializer) {
        this.redisTemplate = redisTemplate;
        this.deserializer = deserializer;
        this.serializer = serializer;
    }

    @Override
    public boolean contains(final ShardKey shard,final String actorId) {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void update(ShardKey shard, PersistentActor persistentActor) throws IOException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void updateAsync(ShardKey shard, PersistentActor persistentActor, InternalMessage message, MessageHandlerEventListener messageHandlerEventListener) {
        // do nothing
    }

    @Override
    public void delete(ShardKey shard, String actorId) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public PersistentActor<ShardKey> get(ShardKey shard, String actorId) throws IOException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }
}
