/*
 * Copyright 2013 the original authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.elasticsoftware.elasticactors.geoevents.messages;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

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
