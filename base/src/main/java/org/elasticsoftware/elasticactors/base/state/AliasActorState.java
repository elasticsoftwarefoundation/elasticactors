package org.elasticsoftware.elasticactors.base.state;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.elasticsoftware.elasticactors.ActorRef;

/**
 * @author Joost van de Wijgerd
 */
public final class AliasActorState extends JacksonActorState<AliasActorState> {
    private final ActorRef aliasedActor;

    @JsonCreator
    public AliasActorState(@JsonProperty("aliasedActor") ActorRef aliasedActor) {
        this.aliasedActor = aliasedActor;
    }

    @Override
    public AliasActorState getBody() {
        return this;
    }

    public ActorRef getAliasedActor() {
        return aliasedActor;
    }
}
