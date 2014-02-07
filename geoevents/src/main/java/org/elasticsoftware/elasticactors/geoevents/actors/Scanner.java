/*
 * Copyright 2013 - 2014 The Original Authors
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
import org.elasticsoftware.elasticactors.TempActor;
import org.elasticsoftware.elasticactors.TypedActor;
import org.elasticsoftware.elasticactors.base.state.JacksonActorState;
import org.elasticsoftware.elasticactors.geoevents.messages.ScanRequest;
import org.elasticsoftware.elasticactors.geoevents.messages.ScanResponse;

import java.util.LinkedList;

/**
 * Temporary Actor used to run a {@link org.elasticsoftware.elasticactors.geoevents.messages.ScanRequest}
 *
 * @author Joost van de Wijgerd
 */
@TempActor(stateClass = Scanner.State.class)
public final class Scanner extends TypedActor<ScanResponse> {
    public static final class State extends JacksonActorState<State> {
        private final ActorRef replyAddress;
        private final ScanRequest request;
        private final ScanResponse response;
        private int runningRequests;

        public State(ScanRequest request, int runningRequests,ActorRef replyAddress) {
            this.replyAddress = replyAddress;
            this.request = request;
            this.runningRequests = runningRequests;
            this.response = new ScanResponse(request.getId(),new LinkedList<ScanResponse.ScanResult>());
        }

        @Override
        public State getBody() {
            return this;
        }
    }

    @Override
    public void onReceive(ActorRef sender, ScanResponse message) throws Exception {
        State state = getState(State.class);
        // merge results
        state.response.merge(message);
        // check for stop condition
        state.runningRequests -= 1;
        if(state.runningRequests == 0) {
            // no more running requests
            state.replyAddress.tell(state.response,getSelf());
            // terminate
            getSystem().stop(getSelf());
        }
    }

    @Override
    public void onUndeliverable(ActorRef receiver, Object message) throws Exception {
        // we are getting an undeliverable, let's create the actor and then resend the message
        String regionId = receiver.getActorId();
        // strip of the regions/
        String geoHash = regionId.substring(8);
        ActorRef regionRef = getSystem().actorOf(regionId,Region.class,new Region.State(GeoHash.fromGeohashString(geoHash)));
        regionRef.tell(message, getSelf());
    }
}
