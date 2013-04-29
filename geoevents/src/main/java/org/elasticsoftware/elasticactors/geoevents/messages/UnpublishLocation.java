package org.elasticsoftware.elasticactors.geoevents.messages;

import org.elasterix.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.geoevents.Coordinate;

/**
 * @author Joost van de Wijgerd
 */
public final class UnPublishLocation {
    private final ActorRef ref;
    private final Coordinate location;

    public UnPublishLocation(ActorRef ref, Coordinate location) {
        this.ref = ref;
        this.location = location;
    }
}
