/*
 * Copyright 2013 - 2017 The Original Authors
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

package org.elasticsoftware.elasticactors.reactivestreams;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.elasticsoftware.elasticactors.ActorRef;

import java.util.ArrayDeque;
import java.util.Queue;

import static org.elasticsoftware.elasticactors.ActorContextHolder.getSelf;

/**
 * @author Joost van de Wijgerd
 */
public final class Subscription {
    private final ActorRef subscriber;
    private final transient Queue<Object> pendingMessages;
    private long demandLeft;

    @JsonCreator
    public Subscription(@JsonProperty("subscriber") ActorRef subscriber,
                        @JsonProperty("demandLeft") long demandLeft) {
        this.subscriber = subscriber;
        this.demandLeft = demandLeft;
        this.pendingMessages = new ArrayDeque<>();
    }

    public ActorRef getSubscriber() {
        return subscriber;
    }

    public long getDemandLeft() {
        return demandLeft;
    }

    public void incrementDemand(long n) {
        this.demandLeft += n;
        tryToPublish();
    }

    private void decrementDemand() {
        this.demandLeft -= 1;
    }

    public void publish(Object message) {
        this.pendingMessages.offer(message);
        tryToPublish();
    }

    private void tryToPublish() {
        if(this.pendingMessages.size() <= this.demandLeft) {
            // send em all
            Object pendingMessage = pendingMessages.poll();
            do {
                subscriber.tell(pendingMessage, getSelf());
                decrementDemand();
                pendingMessage = pendingMessages.poll();
            } while(pendingMessage != null);
        } else if(demandLeft > 0) {
            do {
                subscriber.tell(pendingMessages.poll(), getSelf());
                decrementDemand();
            } while(demandLeft > 0);
        }
    }
}
