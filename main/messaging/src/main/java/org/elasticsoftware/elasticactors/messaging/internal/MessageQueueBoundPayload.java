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

package org.elasticsoftware.elasticactors.messaging.internal;

import jakarta.annotation.Nullable;

/**
 * This interface is used to allow messages sent to a container to still be hashed to
 * the same queue as the messages to specific actors, when using multiple queues per container.
 *
 * e.g. a CreateActorMessage message has to go to the same broker and/or in-memory queue as the
 * messages that are to be sent to the actor that was created, so it can be processed first.
 */
interface MessageQueueBoundPayload {

    @Nullable
    String getMessageQueueAffinityKey();
}
