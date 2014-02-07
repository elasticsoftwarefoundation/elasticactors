package org.elasticsoftware.elasticactors.base.state;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * @author Joost van de Wijgerd
 */
public final class StringState extends JacksonActorState<String> {
    private final String body;

    @JsonCreator
    public StringState(@JsonProperty("body") String body) {
        this.body = body;
    }

    @JsonProperty("body")
    @Override
    public String getBody() {
        return body;
    }
}
