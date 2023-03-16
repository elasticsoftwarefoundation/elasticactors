/*
 * Copyright 2013 - 2023 The Original Authors
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

package org.elasticsoftware.elasticactors.kafka.state;

import org.elasticsoftware.elasticactors.ShardKey;
import org.elasticsoftware.elasticactors.state.PersistentActor;

public interface PersistentActorStore {
    ShardKey getShardKey();

    void put(String actorId, byte[] persistentActorBytes);

    void put(String actorId, byte[] persistentActorBytes, long offset);

    boolean containsKey(String actorId);

    PersistentActor<ShardKey> getPersistentActor(String actorId);

    void remove(String actorId);

    default void init() { }

    default void destroy() {}

    int count();

    default long getOffset() {
        return -1L;
    }

    default boolean isConcurrent() {
        return false;
    }
}
