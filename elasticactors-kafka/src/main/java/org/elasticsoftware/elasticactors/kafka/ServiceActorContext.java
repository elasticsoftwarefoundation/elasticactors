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

import org.elasticsoftware.elasticactors.*;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

public final class ServiceActorContext implements ActorContext {
    private final ActorRef serviceRef;
    private final ActorSystem actorSystem;

    public ServiceActorContext(ActorRef serviceRef, ActorSystem actorSystem) {
        this.serviceRef = serviceRef;
        this.actorSystem = actorSystem;
    }

    @Override
    public ActorRef getSelf() {
        return serviceRef;
    }

    @Override
    public <T extends ActorState> T getState(Class<T> stateClass) {
        return null;
    }

    @Override
    public void setState(ActorState state) {
        // noop
    }

    @Override
    public ActorSystem getActorSystem() {
        return actorSystem;
    }

    @Override
    public Collection<PersistentSubscription> getSubscriptions() {
        return Collections.emptyList();
    }

    @Override
    public Map<String, Set<ActorRef>> getSubscribers() {
        return Collections.emptyMap();
    }
}
