package org.elasticsoftwarefoundation.elasticactors.base.state;

import org.codehaus.jackson.map.ObjectMapper;
import org.elasterix.elasticactors.ActorState;
import org.elasterix.elasticactors.ActorStateFactory;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author Joost van de Wijgerd
 */
public class JacksonActorStateFactory implements ActorStateFactory {
    private final ObjectMapper objectMapper;

    public JacksonActorStateFactory(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public ActorState create() {
        return new JacksonActorState(objectMapper,new LinkedHashMap<String,Object>());
    }

    @Override
    public ActorState create(Map<String, Object> map) {
        return new JacksonActorState(objectMapper,map);
    }

    @Override
    public ActorState create(Object backingObject) {
        return new JacksonActorState(objectMapper,backingObject);
    }
}
