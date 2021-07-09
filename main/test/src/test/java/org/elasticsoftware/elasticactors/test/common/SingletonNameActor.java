package org.elasticsoftware.elasticactors.test.common;

import org.elasticsoftware.elasticactors.Actor;
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.MessageHandler;
import org.elasticsoftware.elasticactors.MethodActor;
import org.elasticsoftware.elasticactors.SingletonActor;
import org.elasticsoftware.elasticactors.base.serialization.JacksonSerializationFramework;
import org.elasticsoftware.elasticactors.state.PersistenceConfig;

@SingletonActor(SingletonNameActor.ACTOR_ID)
@Actor(
        serializationFramework = JacksonSerializationFramework.class,
        stateClass = NameActorState.class)
@PersistenceConfig(excluded = GetActorName.class)
public class SingletonNameActor extends MethodActor {

    public final static String ACTOR_ID = "testSingletonActor";

    @MessageHandler
    public void handleSetActorName(
            ActorRef sender,
            SetActorName setActorName,
            NameActorState state) {
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
            NameActorState state) {
        sender.tell(new CurrentActorName(state.getName()));
    }

}
