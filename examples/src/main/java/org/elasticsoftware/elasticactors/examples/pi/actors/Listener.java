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

package org.elasticsoftware.elasticactors.examples.pi.actors;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.log4j.Logger;
import org.elasticsoftware.elasticactors.Actor;
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.ActorSystem;
import org.elasticsoftware.elasticactors.UntypedActor;
import org.elasticsoftware.elasticactors.base.serialization.JacksonSerializationFramework;
import org.elasticsoftware.elasticactors.base.state.JacksonActorState;
import org.elasticsoftware.elasticactors.examples.pi.messages.Calculate;
import org.elasticsoftware.elasticactors.examples.pi.messages.PiApproximation;
import org.elasticsoftware.elasticactors.http.messages.HttpMessage;
import org.elasticsoftware.elasticactors.http.messages.HttpResponse;
import org.elasticsoftware.elasticactors.http.messages.RegisterRouteMessage;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import java.util.*;

/**
 * @author Joost van de Wijgerd
 */
@Actor(stateClass = Listener.State.class,serializationFramework = JacksonSerializationFramework.class)
public final class Listener extends UntypedActor {
    private static final Logger logger = Logger.getLogger(Listener.class);
    private final ObjectMapper objectMapper = new ObjectMapper();

    public static final class State extends JacksonActorState<Listener.State> {
        private final Map<String,ActorRef> calculations;

        public State() {
            this(new HashMap<String,ActorRef>());
        }

        @JsonCreator
        public State(@JsonProperty("calculations") Map<String, ActorRef> calculations) {
            this.calculations = calculations;
        }

        @Override
        public State getBody() {
            return this;
        }

        @JsonProperty("calculations")
        public Map<String, ActorRef> getCalculations() {
            return calculations;
        }
    }

    @Override
    public void postCreate(ActorRef creator) throws Exception {
        logger.info("Listener.postCreate");
    }

    @Override
    public void postActivate(String previousVersion) throws Exception {
        logger.info("Listener.postActivate");
        // register ourselves with the http server
        ActorSystem httpSystem = getSystem().getParent().get("Http");
        if(httpSystem != null) {
            ActorRef httpServer = httpSystem.serviceActorFor("httpServer");
            httpServer.tell(new RegisterRouteMessage(String.format("/%s", getSelf().getActorId()),getSelf()),getSelf());
        } else {
            logger.warn("Http ActorSystem not available");
        }
    }

    public void onReceive(ActorRef sender, Object message) throws Exception {
        final State state = getState(State.class);
        if(message instanceof HttpMessage) {
            // we have a new calculation request
            String id = UUID.randomUUID().toString();
            state.getCalculations().put(id,sender);
            // tell the master
            ActorRef masterRef = getSystem().actorFor("master");
            masterRef.tell(new Calculate(id),getSelf());
        } else if (message instanceof PiApproximation) {
            PiApproximation approximation = (PiApproximation) message;
            Map<String,List<String>> headers = new HashMap<String,List<String>>();
            headers.put(HttpHeaders.Names.CONTENT_TYPE, Arrays.asList("application/json"));
            HttpResponse httpResponse = new HttpResponse(HttpResponseStatus.OK.getCode(),
                                                         headers,
                                                         objectMapper.writeValueAsBytes(approximation));
            // get the recipient
            ActorRef replyRef = state.getCalculations().remove(approximation.getCalculationId());
            replyRef.tell(httpResponse,getSelf());
        } else {
            unhandled(message);
        }
    }


}
