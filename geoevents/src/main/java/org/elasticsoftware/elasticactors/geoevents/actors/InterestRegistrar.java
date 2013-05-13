/*
 * Copyright 2013 Joost van de Wijgerd
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

package org.elasticsoftware.elasticactors.geoevents.actors;

import ch.hsr.geohash.GeoHash;
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.UntypedActor;
import org.elasticsoftware.elasticactors.geoevents.Coordinate;
import org.elasticsoftware.elasticactors.geoevents.messages.DeRegisterInterest;
import org.elasticsoftware.elasticactors.geoevents.messages.PublishLocation;
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
        } else if(message instanceof PublishLocation) {
            handle((PublishLocation) message);
        } else if(message instanceof DeRegisterInterest) {
            handle((DeRegisterInterest) message);
        } else {
            unhandled(message);
        }
    }

    private void handle(PublishLocation message) {
        Coordinate location = message.getLocation();
        GeoHash regionHash = getRegion(location);

        getSystem().actorFor(String.format("regions/%s",regionHash.toBase32())).tell(message,getSelf());
    }

    private void handle(RegisterInterest message) {
        // determine what geohashes to aim for
        Coordinate location = message.getLocation();
        GeoHash regionHash = getRegion(location);
        // find all regions that need to register this interest
        List<GeoHash> allRegions =  findAllRegions(regionHash,message.getLocation(),message.getRadiusInMetres());
        for (GeoHash region : allRegions) {
            getSystem().actorFor(String.format("regions/%s",region.toBase32())).tell(message,getSelf());
        }
    }

    private void handle(DeRegisterInterest message) {
        Coordinate location = message.getLocation();
        GeoHash regionHash = getRegion(location);
        getSystem().actorFor(String.format("regions/%s",regionHash.toBase32())).tell(message,getSelf());
    }

    private GeoHash getRegion(Coordinate location) {
        GeoHash locationHash = GeoHash.withCharacterPrecision(location.getLatitude(),location.getLongitude(),12);
        // we work with regions of three characters
        return GeoHash.fromGeohashString(locationHash.toBase32().substring(0,3));
    }

    private List<GeoHash> findAllRegions(GeoHash regionHash, Coordinate location, int radiusInMetres) {
        // for now return all adjecent neighbours
        // @todo: need a function here that calculates the bounding box and only inlcudes the overlapping regions
        // return Arrays.asList(regionHash.getAdjacent());
        return Arrays.asList(regionHash);
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
