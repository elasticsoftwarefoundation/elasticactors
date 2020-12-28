package org.elasticsoftware.elasticactors.test.common;

import org.elasticsoftware.elasticactors.Actor;
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.MessageHandler;
import org.elasticsoftware.elasticactors.MethodActor;
import org.elasticsoftware.elasticactors.SingletonActor;
import org.elasticsoftware.elasticactors.base.serialization.JacksonSerializationFramework;
import org.elasticsoftware.elasticactors.state.PersistenceConfig;

@SingletonActor(SingletonNameActorState.ACTOR_ID)
@Actor(
        serializationFramework = JacksonSerializationFramework.class,
        stateClass = SingletonNameActorState.class)
@PersistenceConfig(excluded = GetActorName.class)
public class SingletonNameActor extends MethodActor {

    @MessageHandler
    public void handleSetActorName(
            ActorRef sender,
            SetActorName setActorName,
            SingletonNameActorState state) {
        logger.info(
                "Changing actor name. Old: '{}'. New: '{}'",
                state.getName(),
                setActorName.getNewName());
        CurrentActorName currentActorName = new CurrentActorName(state.getName());
        state.setName(setActorName.getNewName());
        if (sender != null) {
            sender.tell(currentActorName);
        }
    }

    @MessageHandler
    public void handleGetActorName(
            ActorRef sender,
            GetActorName getActorName,
            SingletonNameActorState state) {
        sender.tell(new CurrentActorName(state.getName()));
    }

}
