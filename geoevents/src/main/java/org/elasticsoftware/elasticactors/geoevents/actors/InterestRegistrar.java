package org.elasticsoftware.elasticactors.geoevents.actors;

import ch.hsr.geohash.GeoHash;
import org.elasterix.elasticactors.ActorRef;
import org.elasterix.elasticactors.UntypedActor;
import org.elasticsoftware.elasticactors.geoevents.Coordinate;
import org.elasticsoftware.elasticactors.geoevents.messages.RegisterInterest;

import java.util.Arrays;
import java.util.List;

/**
 * @author Joost van de Wijgerd
 */
public final class InterestRegistrar extends UntypedActor {
    @Override
    public void onReceive(ActorRef sender, Object message) throws Exception {
        if(message instanceof RegisterInterest) {
            handle((RegisterInterest) message);
        }
    }

    private void handle(RegisterInterest message) {
        // determine what geohashes to aim for
        Coordinate location = message.getLocation();
        GeoHash locationHash = GeoHash.withCharacterPrecision(location.getLatitude(),location.getLongitude(),12);
        // we work with regions of three characters
        GeoHash regionHash = GeoHash.fromGeohashString(locationHash.toBase32().substring(0,2));
        // find all regions that need to register this interest
        List<GeoHash> allRegions =  findAllRegions(regionHash,message.getLocation(),message.getRadiusInMetres());
        for (GeoHash region : allRegions) {
            getSystem().actorFor(String.format("regions/%s",region.toBase32())).tell(message,getSelf());
        }
    }

    private List<GeoHash> findAllRegions(GeoHash regionHash, Coordinate location, int radiusInMetres) {
        // for now return all adjecent neighbours
        // @todo: need a function here that calculates the bounding box and only inlcudes the overlapping regions
        return Arrays.asList(regionHash.getAdjacent());
    }

    @Override
    public void onUndeliverable(ActorRef receiver, Object message) throws Exception {
        // we are getting an undeliverable, let's create the actor and then resend the message
        String regionId = receiver.getActorId();
        // strip of the regions/
        String geoHash = regionId.substring(8);
        ActorRef regionRef = getSystem().actorOf(regionId,Region.class,new Region.State(GeoHash.fromGeohashString(geoHash)));
        regionRef.tell(message,getSelf());
    }
}
