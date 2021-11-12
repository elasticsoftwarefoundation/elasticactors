package org.elasticsoftware.elasticactors.messaging.internal;

import javax.annotation.Nullable;

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
