package org.elasticsoftware.elasticactors.base.actors;

import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.TempActor;
import org.elasticsoftware.elasticactors.TypedActor;

/**
 * @author Joost van de Wijgerd
 */
@TempActor(stateClass = ActorDelegate.class)
public final class ReplyActor<T> extends TypedActor<T> {
    @Override
    public void onUndeliverable(ActorRef receiver, Object message) throws Exception {
        getState(ActorDelegate.class).onUndeliverable(receiver,message);
    }

    @Override
    public void onReceive(ActorRef sender, T message) throws Exception {
        getState(ActorDelegate.class).onReceive(sender, message);
    }
}
