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

package org.elasticsoftware.elasticactors;

import jakarta.annotation.Nonnull;

/**
 * @author Joost van de Wijgerd
 */
public interface SubscriberContext {
    @Nonnull
    ActorRef getSelf();

    @Nonnull
    ActorRef getPublisher();

    @Nonnull
    <T extends ActorState> T getState(Class<T> stateClass);

    @Nonnull
    ActorSystem getActorSystem();

    @Nonnull
    PersistentSubscription getSubscription();
}
