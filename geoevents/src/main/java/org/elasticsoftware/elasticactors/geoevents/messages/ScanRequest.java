package org.elasticsoftware.elasticactors.geoevents.messages;

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;
import org.elasticsoftware.elasticactors.geoevents.Coordinate;

import java.util.UUID;

/**
 * @author Joost van de Wijgerd
 */
public final class ScanRequest {
    private final UUID id;
    private final Coordinate location;
    private final int radiusInMetres;

    public ScanRequest(Coordinate location, int radiusInMetres) {
        this(UUID.randomUUID(),location,radiusInMetres);
    }

    @JsonCreator
    public ScanRequest(@JsonProperty("id") UUID id,@JsonProperty("location") Coordinate location, @JsonProperty("radiusInMetres") int radiusInMetres) {
        this.id = id;
        this.location = location;
        this.radiusInMetres = radiusInMetres;
    }

    @JsonProperty("id")
    public UUID getId() {
        return id;
    }

    @JsonProperty("location")
    public Coordinate getLocation() {
        return location;
    }

    @JsonProperty("radiusInMetres")
    public int getRadiusInMetres() {
        return radiusInMetres;
    }
}
