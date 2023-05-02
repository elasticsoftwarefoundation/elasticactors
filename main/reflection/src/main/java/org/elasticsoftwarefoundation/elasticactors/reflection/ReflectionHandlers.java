package org.elasticsoftwarefoundation.elasticactors.reflection;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.fge.jsonpatch.JsonPatchException;
import org.elasticsoftware.elasticactors.ActorContextHolder;
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.ActorSystem;
import org.elasticsoftware.elasticactors.MessageHandler;
import org.elasticsoftware.elasticactors.base.serialization.JacksonSerializationFramework;
import org.elasticsoftware.elasticactors.base.state.JacksonActorState;
import org.elasticsoftware.elasticactors.cluster.InternalActorSystem;
import org.elasticsoftware.elasticactors.state.PersistenceConfig;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

@PersistenceConfig(persistOnMessages = false, included = ApplyJsonPatch.class)
public class ReflectionHandlers {
    public static final ReflectionHandlers INSTANCE = new ReflectionHandlers();

    @MessageHandler
    public void handleGetSerializedState(
        SerializedStateRequest message,
        JacksonActorState state,
        ActorSystem actorSystem,
        ActorRef sender) throws IOException {

        byte[] serializedBytes = ((InternalActorSystem) actorSystem).getParent()
            .getSerializationFramework(state.getSerializationFramework())
            .getActorStateSerializer(state).serialize(state);

        sender.tell(new SerializedStateResponse(
            new String(serializedBytes, StandardCharsets.UTF_8)));
    }

    @MessageHandler
    public void handlePatchState(
        ApplyJsonPatch patch,
        JacksonActorState state,
        ActorSystem actorSystem,
        ActorRef sender) throws IOException, JsonPatchException {

        byte[] serializedBytes = serializeState(actorSystem, state);
        ObjectMapper objectMapper = getObjectMapper(actorSystem, state);
        JsonNode jsonNode = objectMapper.readTree(serializedBytes);
        jsonNode = patch.getPatch().apply(jsonNode);

        byte[] patchedBytes = objectMapper.writeValueAsBytes(jsonNode);
        JacksonActorState jacksonActorState =
            objectMapper.readValue(patchedBytes, state.getClass());

        if (!patch.isDryRun()) {
            ActorContextHolder.setState(jacksonActorState);
        }

        sender.tell(new SerializedStateResponse(
            new String(patchedBytes, StandardCharsets.UTF_8)));
    }

    private byte[] serializeState(ActorSystem actorSystem, JacksonActorState state)
        throws IOException {

        return ((InternalActorSystem) actorSystem).getParent()
            .getSerializationFramework(state.getSerializationFramework())
            .getActorStateSerializer(state).serialize(state);
    }

    private ObjectMapper getObjectMapper(ActorSystem actorSystem, JacksonActorState state) {
        JacksonSerializationFramework serializationFramework =
            (JacksonSerializationFramework) ((InternalActorSystem) actorSystem).getParent()
                .getSerializationFramework(state.getSerializationFramework());
        return serializationFramework.getObjectMapper();
    }
}
