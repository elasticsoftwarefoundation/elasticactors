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

package org.elasticsoftware.elasticactors.cache;

import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.ShardKey;
import org.elasticsoftware.elasticactors.cluster.metrics.MicrometerConfiguration;
import org.elasticsoftware.elasticactors.state.PersistentActor;

import javax.annotation.Nullable;

/**
 * @author Joost van de Wijgerd
 */
public final class ShardActorCacheManager extends CacheManager<ActorRef,PersistentActor<ShardKey>> {
    public ShardActorCacheManager(int maximumSize, @Nullable
        MicrometerConfiguration micrometerConfiguration) {
        super(maximumSize, micrometerConfiguration);
    }
}
