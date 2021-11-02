package org.elasticsoftware.elasticactors.messaging.internal;

import javax.annotation.Nullable;

/**
 * FOR INTERNAL USAGE ONLY.
 * Implementing this in other message types will have no effect.
 *
 * This interface is used to allow messages sent to shard or nodes themselves to still be hashed to
 * the same queue as the messages to specific actors.
 *
 * Ex.: a CreateActorMessage message has to go to the same broker and/or in-memory queue as the
 * messages that are to be sent to the actor that was created.
 */
public interface Hashable<T> {

    /**
     * Return the object as a Hashable if, and only if, its class is in the same package as Hashable.
     * Otherwise, return null.
     */
    @Nullable
    static <A> Hashable<A> getIfHashable(@Nullable Object object) {
        return object instanceof Hashable
            && Hashable.class.getPackage().getName().equals(object.getClass().getPackage().getName())
            ? (Hashable<A>) object
            : null;
    }

    @Nullable
    T getHashKey();
}
