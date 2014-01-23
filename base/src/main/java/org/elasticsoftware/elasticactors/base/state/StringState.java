package org.elasticsoftware.elasticactors.base.state;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * @author Joost van de Wijgerd
 */
public final class StringState extends JacksonActorState<Void,String> {
    private final String body;

    @JsonCreator
    public StringState(@JsonProperty("body") String body) {
        this.body = body;
    }

    @Override
    public Void getId() {
        return null;
    }

    @JsonProperty("body")
    @Override
    public String getBody() {
        return body;
    }
}
