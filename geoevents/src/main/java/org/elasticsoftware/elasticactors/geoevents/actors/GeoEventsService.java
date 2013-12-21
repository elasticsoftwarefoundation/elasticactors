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

package org.elasticsoftware.elasticactors.geoevents.actors;

import ch.hsr.geohash.GeoHash;
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.ServiceActor;
import org.elasticsoftware.elasticactors.UntypedActor;
import org.elasticsoftware.elasticactors.geoevents.Coordinate;
import org.elasticsoftware.elasticactors.geoevents.LengthUnit;
import org.elasticsoftware.elasticactors.geoevents.messages.*;
import org.elasticsoftware.elasticactors.geoevents.util.GeoHashUtils;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author Joost van de Wijgerd
 */
@ServiceActor("geoEventsService")
public final class GeoEventsService extends UntypedActor {
    public static final String REGIONS_FORMAT = "regions/%s";

    @Override
    public void onReceive(ActorRef sender, Object message) throws Exception {
        if(message instanceof RegisterInterest) {
            handle((RegisterInterest) message);
        } else if(message instanceof PublishLocation) {
            handle((PublishLocation) message);
        } else if(message instanceof DeRegisterInterest) {
            handle((DeRegisterInterest) message);
        } else if(message instanceof UnpublishLocation) {
            handle((UnpublishLocation) message);
        } else if(message instanceof ScanRequest) {
            handle((ScanRequest) message,sender);
        } else {
            unhandled(message);
        }
    }

    private void handle(ScanRequest message,ActorRef replyAddress) throws Exception {
        List<GeoHash> allRegions = findAllRegions(message.getLocation(),message.getRadiusInMetres());
        // create a temp actor to handle the results
        ActorRef scanner = getSystem().tempActorOf(Scanner.class,new Scanner.State(message,allRegions.size(),replyAddress));
        for (GeoHash region : allRegions) {
            getSystem().actorFor(String.format(REGIONS_FORMAT,region.toBase32())).tell(message,scanner);
        }
    }

    private void handle(UnpublishLocation message) {
        Coordinate location = message.getLocation();
        GeoHash regionHash = getRegion(location);

        getSystem().actorFor(String.format(REGIONS_FORMAT,regionHash.toBase32())).tell(message,getSelf());
    }

    private void handle(PublishLocation message) {
        Coordinate location = message.getLocation();
        GeoHash regionHash = getRegion(location);

        getSystem().actorFor(String.format(REGIONS_FORMAT,regionHash.toBase32())).tell(message,getSelf());
    }

    private void handle(RegisterInterest message) {
        // determine what geohashes to aim for
        Coordinate location = message.getLocation();
        // find all regions that need to register this interest
        List<GeoHash> allRegions =  findAllRegions(message.getLocation(),message.getRadiusInMetres());
        for (GeoHash region : allRegions) {
            getSystem().actorFor(String.format(REGIONS_FORMAT,region.toBase32())).tell(message,getSelf());
        }
    }

    private void handle(DeRegisterInterest message) {
        Coordinate location = message.getLocation();
        GeoHash regionHash = getRegion(location);
        getSystem().actorFor(String.format(REGIONS_FORMAT,regionHash.toBase32())).tell(message,getSelf());
    }

    private GeoHash getRegion(Coordinate location) {
        GeoHash locationHash = GeoHash.withCharacterPrecision(location.getLatitude(),location.getLongitude(),12);
        // we work with regions of three characters
        return GeoHash.fromGeohashString(locationHash.toBase32().substring(0,3));
    }

    private List<GeoHash> findAllRegions(Coordinate location, int radiusInMetres) {
        return GeoHashUtils.getAllGeoHashesWithinRadius(location.getLatitude(),
                                                        location.getLongitude(),
                                                        (double) radiusInMetres,
                                                        LengthUnit.METRES,
                                                        3);
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
