package org.elasticsoftware.elasticactors.geoevents.messages;

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import java.util.List;
import java.util.UUID;

/**
 * @author Joost van de Wijgerd
 */
public final class ScanResponse {
    private final UUID requestId;
    private final List<ScanResult> results;

    @JsonCreator
    public ScanResponse(UUID requestId, List<ScanResult> results) {
        this.requestId = requestId;
        this.results = results;
    }

    public void merge(ScanResponse otherResponse) {
        this.results.addAll(otherResponse.results);
    }

    @JsonProperty("requestId")
    public UUID getRequestId() {
        return requestId;
    }

    @JsonProperty("results")
    public List<ScanResult> getResults() {
        return results;
    }

    public static final class ScanResult {
        private final PublishLocation publishedLocation;
        private final int distanceInMetres;

        @JsonCreator
        public ScanResult(@JsonProperty("publishedLocations") PublishLocation publishedLocation,
                          @JsonProperty("distanceInMetres") int distanceInMetres) {
            this.publishedLocation = publishedLocation;
            this.distanceInMetres = distanceInMetres;
        }

        @JsonProperty("publishedLocations")
        public PublishLocation getPublishedLocation() {
            return publishedLocation;
        }

        @JsonProperty("distanceInMetres")
        public int getDistanceInMetres() {
            return distanceInMetres;
        }
    }
}
