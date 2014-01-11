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
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.log4j.Logger;
import org.elasticsoftware.elasticactors.Actor;
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.UntypedActor;
import org.elasticsoftware.elasticactors.base.serialization.JacksonSerializationFramework;
import org.elasticsoftware.elasticactors.base.state.JacksonActorState;
import org.elasticsoftware.elasticactors.examples.pi.messages.Calculate;
import org.elasticsoftware.elasticactors.examples.pi.messages.PiApproximation;
import org.elasticsoftware.elasticactors.examples.pi.messages.Result;
import org.elasticsoftware.elasticactors.examples.pi.messages.Work;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Joost van de Wijgerd
 */
@Actor(stateClass = Master.State.class,serializationFramework = JacksonSerializationFramework.class)
public final class  Master extends UntypedActor  {
    private static final Logger logger = Logger.getLogger(Master.class);

    public static final class State extends JacksonActorState<String,State> {
        private final ActorRef listener;
        private final int nrOfWorkers;
        private final int nrOfMessages;
        private final int nrOfElements;
        private List<ActorRef> workers;
        private final Map<String,Calculation> calculations;

        private static final class Calculation {
            private final long start;
            private double pi;
            private int nrOfResults;
            private int roundRobinCounter;

            private Calculation() {
                this(System.currentTimeMillis(),0.0d,0,0);
            }

            @JsonCreator
            public Calculation(@JsonProperty("start") long start,
                               @JsonProperty("pi") double pi,
                               @JsonProperty("nrOfResults") int nrOfResults,
                               @JsonProperty("roundRobinCounter") int roundRobinCounter) {
                this.start = start;
                this.pi = pi;
                this.nrOfResults = nrOfResults;
                this.roundRobinCounter = roundRobinCounter;
            }

            @JsonProperty("start")
            public long getStart() {
                return start;
            }

            @JsonProperty("pi")
            public double getPi() {
                return pi;
            }

            public void setPi(double pi) {
                this.pi = pi;
            }

            public void incrementPi(double pi) {
                this.pi += pi;
            }

            @JsonProperty("nrOfResults")
            public int getNrOfResults() {
                return nrOfResults;
            }

            public void setNrOfResults(int nrOfResults) {
                this.nrOfResults = nrOfResults;
            }

            public void incrementNumberOfResults() {
                this.nrOfResults += 1;
            }

            @JsonProperty("roundRobinCounter")
            public int getRoundRobinCounter() {
                return roundRobinCounter;
            }

            @JsonIgnore
            public int getAndIncrementRoundRobinCounter() {
                final int currentValue = roundRobinCounter;
                roundRobinCounter+=1;
                return currentValue;
            }

            public void setRoundRobinCounter(int roundRobinCounter) {
                this.roundRobinCounter = roundRobinCounter;
            }
        }

        public State(ActorRef listener, int nrOfWorkers, int nrOfMessages, int nrOfElements) {
            this(listener, nrOfWorkers, nrOfMessages, nrOfElements, new HashMap<String,Calculation>());
        }

        @JsonCreator
        public State(@JsonProperty("listener") ActorRef listener,
                     @JsonProperty("nrOfWorkers") int nrOfWorkers,
                     @JsonProperty("nrOfMessages") int nrOfMessages,
                     @JsonProperty("nrOfElements") int nrOfElements,
                     @JsonProperty("calculations") Map<String,Calculation> calculations) {
            this.listener = listener;
            this.nrOfWorkers = nrOfWorkers;
            this.nrOfMessages = nrOfMessages;
            this.nrOfElements = nrOfElements;
            this.calculations = calculations;
        }

        @Override
        public String getId() {
            return null;
        }

        @Override
        public State getBody() {
            return this;
        }

        @JsonProperty("listener")
        public ActorRef getListener() {
            return listener;
        }

        @JsonProperty("workers")
        public List<ActorRef> getWorkers() {
            return workers;
        }

        public void setWorkers(List<ActorRef> workers) {
            this.workers = workers;
        }

        @JsonProperty("nrOfWorkers")
        public int getNrOfWorkers() {
            return nrOfWorkers;
        }

        @JsonProperty("nrOfMessages")
        public int getNrOfMessages() {
            return nrOfMessages;
        }

        @JsonProperty("nrOfElements")
        public int getNrOfElements() {
            return nrOfElements;
        }

        @JsonProperty("calculations")
        public Map<String,Calculation> getCalculations() {
            return calculations;
        }
    }

    @Override
    public void postCreate(ActorRef creator) throws Exception {
        State state = getState(State.class);
        logger.info(String.format("Master.postCreate -> listener ref: %s",state.getListener()));
        List<ActorRef> workers = new ArrayList<ActorRef>(state.getNrOfWorkers());
        for (int i = 1; i <= state.getNrOfWorkers(); i++) {
            workers.add(getSystem().actorOf("worker-"+i,Worker.class));
        }
        state.setWorkers(workers);
    }

    @Override
    public void postActivate(String previousVersion) throws Exception {
    }

    public void onReceive(ActorRef sender, Object message) {
        State state = getState(State.class);
        if (message instanceof Calculate) {
            logger.info("Starting calculation");
            Calculate calculateRequest = (Calculate) message;
            State.Calculation calculation = new State.Calculation();
            for (int start = 0; start < state.getNrOfMessages(); start++) {
                state.getWorkers().get(calculation.roundRobinCounter++ % state.nrOfWorkers).tell(new Work(start, state.getNrOfElements(), calculateRequest.getId()), getSelf());
            }
            state.getCalculations().put(calculateRequest.getId(), calculation);
        } else if (message instanceof Result) {
            Result result = (Result) message;
            State.Calculation calculation = state.getCalculations().get(result.getCalculationId());
            calculation.incrementPi(result.getValue());
            calculation.incrementNumberOfResults();

            if (calculation.getNrOfResults() == state.getNrOfMessages()) {
                logger.info(String.format("Calculation done, sending reply to actor [%s]",state.getListener()));
                // Send the result to the listener
                long duration = System.currentTimeMillis() - calculation.getStart();
                state.getListener().tell(new PiApproximation(result.getCalculationId(), calculation.getPi(), duration), getSelf());
                // remove calculation
                state.getCalculations().remove(result.getCalculationId());
                // Stops this actor and all its supervised children
                // @todo: figure out how to do this
                // getContext().stop(getSelf());
            }
        } else {
            unhandled(message);
        }
    }
}
