package org.elasticsoftware.elasticactors.test.common;

import org.elasticsoftware.elasticactors.Actor;
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.ManagedActor;
import org.elasticsoftware.elasticactors.MessageHandler;
import org.elasticsoftware.elasticactors.MethodActor;
import org.elasticsoftware.elasticactors.base.serialization.JacksonSerializationFramework;
import org.elasticsoftware.elasticactors.state.PersistenceConfig;

@ManagedActor(
        value = {
                ExclusiveManagedNameActor.ACTOR_ID_0,
                ExclusiveManagedNameActor.ACTOR_ID_1,
                ExclusiveManagedNameActor.ACTOR_ID_2
        },
        initialStateProvider = InitialNameStateProvider.class)
@Actor(
        serializationFramework = JacksonSerializationFramework.class,
        stateClass = NameActorState.class)
@PersistenceConfig(excluded = GetActorName.class)
public class ExclusiveManagedNameActor extends MethodActor {

    public static final String ACTOR_ID_0 = "exclusiveManagedNameActor0";
    public static final String ACTOR_ID_1 = "exclusiveManagedNameActor1";
    public static final String ACTOR_ID_2 = "exclusiveManagedNameActor2";

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
