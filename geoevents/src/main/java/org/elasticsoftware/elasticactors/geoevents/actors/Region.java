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
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.UntypedActor;
import org.elasticsoftware.elasticactors.geoevents.LengthUnit;
import org.elasticsoftware.elasticactors.geoevents.messages.DeRegisterInterest;
import org.elasticsoftware.elasticactors.geoevents.messages.EnterEvent;
import org.elasticsoftware.elasticactors.geoevents.messages.PublishLocation;
import org.elasticsoftware.elasticactors.geoevents.messages.RegisterInterest;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * @author Joost van de Wijgerd
 */
public final class Region extends UntypedActor {
    public static final class State {
        private final GeoHash geoHash;
        private final List<RegisterInterest> listeners;
        private final SortedMap<Long,PublishLocation> publishedLocations;

        public State(GeoHash geoHash) {
            this(geoHash,new LinkedList<RegisterInterest>(),new TreeMap<Long,PublishLocation>());
        }

        @JsonCreator
        public State(@JsonProperty("geoHash") GeoHash geoHash,
                     @JsonProperty("listeners") List<RegisterInterest> listeners,
                     @JsonProperty("publishedLocations") SortedMap<Long,PublishLocation> publishedLocations) {
            this.geoHash = geoHash;
            this.listeners = listeners;
            this.publishedLocations = publishedLocations;
        }

        @JsonProperty("geoHash")
        public GeoHash getGeoHash() {
            return geoHash;
        }

        @JsonProperty("listeners")
        public List<RegisterInterest> getListeners() {
            return listeners;
        }

        @JsonProperty("publishedLocations")
        public SortedMap<Long,PublishLocation> getPublishedLocations() {
            return publishedLocations;
        }

        public SortedMap<Long,PublishLocation> prunePublishedLocations(Long now) {
            SortedMap<Long,PublishLocation> tailMap = publishedLocations.tailMap(now);
            // prune the head
            Iterator itr = publishedLocations.headMap(now).entrySet().iterator();
            while(itr.hasNext()) {
                itr.next();
                itr.remove();
            }
            return tailMap;
        }
    }

    @Override
    public void onReceive(ActorRef sender, Object message) throws Exception {
        if(message instanceof PublishLocation) {
            handle((PublishLocation) message);
        } else if(message instanceof RegisterInterest) {
            handle((RegisterInterest) message);
        } else if(message instanceof DeRegisterInterest) {
            handle((DeRegisterInterest) message);
        }
    }

    private void handle(PublishLocation message) {
        // generate lastSeen for new publish event
        long lastSeen = System.currentTimeMillis();
        // iterate through all interested listeners
        State state = getState(null).getAsObject(State.class);
        for (RegisterInterest listener : state.getListeners()) {
            if(!listener.getActorRef().equals(message.getRef())) {
                double distance = listener.getLocation().distance(message.getLocation(), LengthUnit.METRES);
                if(distance <= listener.getRadiusInMetres()) {
                    fireEnterEvent(listener.getActorRef(),message,(int)distance,lastSeen);
                }
            }
        }
        // add to the events map
        state.publishedLocations.put(lastSeen+TimeUnit.MILLISECONDS.convert(message.getTtlInSeconds(),TimeUnit.SECONDS),message);
    }

    private void fireEnterEvent(ActorRef receiver,PublishLocation originalMessage,int distance,long lastSeen) {
        receiver.tell(new EnterEvent(originalMessage.getRef(),
                                     originalMessage.getLocation(),
                                     distance,
                                     lastSeen,
                                     originalMessage.getCustomProperties()),getSelf());
    }

    private void handle(RegisterInterest message) {
        // add interest
        State state = getState(null).getAsObject(State.class);
        state.getListeners().add(message);
        // iterate over the (pruned) previous published locations
        state.prunePublishedLocations(System.currentTimeMillis());
    }
    
    private void handle(DeRegisterInterest message) {
        State state = getState(null).getAsObject(State.class);
        ListIterator<RegisterInterest> itr = state.getListeners().listIterator();
        while (itr.hasNext()) {
            RegisterInterest registeredInterest = itr.next();
            if(registeredInterest.getActorRef().equals(message.getActorRef()) &&
               registeredInterest.getLocation().equals(message.getLocation())) {
                itr.remove();
                removeFromOtherRegions(registeredInterest);
            }
        }
    }

    private void removeFromOtherRegions(RegisterInterest registeredInterest) {
        //@todo: based on the radius, the location and the the current region find any other regions that
        //@todo: have registered the interest
    }

}
