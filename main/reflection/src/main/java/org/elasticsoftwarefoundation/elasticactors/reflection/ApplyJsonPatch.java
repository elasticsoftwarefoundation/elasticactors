package org.elasticsoftwarefoundation.elasticactors.reflection;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.github.fge.jsonpatch.JsonPatch;
import org.elasticsoftware.elasticactors.base.serialization.JacksonSerializationFramework;
import org.elasticsoftware.elasticactors.serialization.Message;

@Message(
    serializationFramework = JacksonSerializationFramework.class,
    durable = false,
    immutable = true)
public class ApplyJsonPatch {
    private final JsonPatch patch;

    @JsonCreator
    public ApplyJsonPatch(@JsonProperty("patch") JsonPatch patch) {
        this.patch = patch;
    }

    public JsonPatch getPatch() {
        return patch;
    }
}
