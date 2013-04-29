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
import org.codehaus.jackson.annotate.JsonProperty;
import org.elasterix.elasticactors.ActorRef;
import org.elasterix.elasticactors.UntypedActor;

import java.util.List;

/**
 * @author Joost van de Wijgerd
 */
public final class BoundingBox extends UntypedActor {
    public static final class State {
        private final GeoHash myHash;
        private final List<Interest> listeners;

        public State(GeoHash myHash, List<Interest> listeners) {
            this.myHash = myHash;
            this.listeners = listeners;
        }

        public GeoHash getMyHash() {
            return myHash;
        }

        public List<Interest> getListeners() {
            return listeners;
        }
    }

    public static final class Interest {
        private final ActorRef actorRef;
        private final GeoHash location;
        private final int radius;

        public Interest(ActorRef actorRef, GeoHash location, int radius) {
            this.actorRef = actorRef;
            this.location = location;
            this.radius = radius;
        }
    }

    @Override
    public void onReceive(ActorRef sender, Object message) throws Exception {

    }
}
