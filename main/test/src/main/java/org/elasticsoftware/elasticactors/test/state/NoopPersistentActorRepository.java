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

package org.elasticsoftware.elasticactors.test.state;

import org.elasticsoftware.elasticactors.ShardKey;
import org.elasticsoftware.elasticactors.messaging.InternalMessage;
import org.elasticsoftware.elasticactors.messaging.MessageHandlerEventListener;
import org.elasticsoftware.elasticactors.state.PersistentActor;
import org.elasticsoftware.elasticactors.state.PersistentActorRepository;

import java.io.IOException;

/**
 * @author Joost van de Wijgerd
 */
public final class NoopPersistentActorRepository implements PersistentActorRepository {
    @Override
    public boolean contains(ShardKey shard, String actorId) {
        return false;
    }

    @Override
    public void update(ShardKey shard, PersistentActor persistentActor) throws IOException {
        // do nothing
    }

    @Override
    public void updateAsync(ShardKey shard, PersistentActor persistentActor, InternalMessage message, MessageHandlerEventListener messageHandlerEventListener) {
        // do nothing
    }

    @Override
    public void delete(ShardKey shard, String actorId) {
       // do nothing
    }

    @Override
    public PersistentActor<ShardKey> get(ShardKey shard, String actorId) throws IOException {
        return null;
    }
}
