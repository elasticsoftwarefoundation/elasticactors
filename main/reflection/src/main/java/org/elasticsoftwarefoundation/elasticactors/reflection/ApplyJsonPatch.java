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
    private final boolean dryRun;

    @JsonCreator
    public ApplyJsonPatch(
        @JsonProperty("patch") JsonPatch patch,
        @JsonProperty("dryRun") boolean dryRun
    ) {
        this.patch = patch;
        this.dryRun = dryRun;
    }

    public JsonPatch getPatch() {
        return patch;
    }

    public boolean isDryRun() {
        return dryRun;
    }
}
