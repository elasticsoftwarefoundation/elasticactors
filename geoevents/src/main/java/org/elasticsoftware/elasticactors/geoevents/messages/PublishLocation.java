package org.elasticsoftware.elasticactors.geoevents.messages;

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;
import org.elasterix.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.geoevents.Coordinate;

import java.util.Map;

/**
 * @author Joost van de Wijgerd
 */
public final class PublishLocation {
    private final ActorRef ref;
    private final Coordinate location;
    private final long ttlInSeconds;
    private final Map<String,Object> customProperties;

    @JsonCreator
    public PublishLocation(@JsonProperty("ref") ActorRef ref,
                           @JsonProperty("location") Coordinate location,
                           @JsonProperty("ttlInSeconds") long ttlInSeconds,
                           @JsonProperty("customProperties") Map<String, Object> customProperties) {
        this.ref = ref;
        this.location = location;
        this.ttlInSeconds = ttlInSeconds;
        this.customProperties = customProperties;
    }

    @JsonProperty("ref")
    public ActorRef getRef() {
        return ref;
    }

    @JsonProperty("location")
    public Coordinate getLocation() {
        return location;
    }

    @JsonProperty("ttlInSeconds")
    public long getTtlInSeconds() {
        return ttlInSeconds;
    }

    @JsonProperty("customProperties")
    public Map<String, Object> getCustomProperties() {
        return customProperties;
    }
}
