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

package org.elasticsoftware.elasticactors.kafka;

import com.google.common.cache.Cache;
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.NodeKey;
import org.elasticsoftware.elasticactors.ShardKey;
import org.elasticsoftware.elasticactors.state.PersistentActor;

public interface ManagedActorContainer<K> {
    PersistentActor<K> getPersistentActor(ActorRef actorRef);

    void persistActor(PersistentActor<K> persistentActor);

    void deleteActor(PersistentActor<K> persistentActor);

    boolean containsKey(String actorId);

    K getKey();

    Cache<ActorRef, PersistentActor<K>> getActorCache();
}
