package org.elasticsoftware.elasticactors.test.common;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.elasticsoftware.elasticactors.base.state.JacksonActorState;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class SingletonNameActorState extends JacksonActorState<SingletonNameActorState> {

    public final static String ACTOR_ID = "testSingletonActor";
    public static final String DEFAULT_NAME = "DEFAULT_NAME";

    private String name;

    public SingletonNameActorState() {
        this(DEFAULT_NAME);
    }

    @JsonCreator
    public SingletonNameActorState(@JsonProperty("name") String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public SingletonNameActorState getBody() {
        return this;
    }
}
