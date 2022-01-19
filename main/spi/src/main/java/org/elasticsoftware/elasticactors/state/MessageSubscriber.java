/*
 *   Copyright 2013 - 2022 The Original Authors
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.elasticsoftware.elasticactors.state;

import org.elasticsoftware.elasticactors.ActorRef;

import static java.lang.Math.addExact;

/**
 * @author Joost van de Wijgerd
 */
public final class MessageSubscriber {
    private final ActorRef subscriberRef;
    private long leases;

    public MessageSubscriber(ActorRef subscriberRef) {
        this(subscriberRef, 0);
    }

    public MessageSubscriber(ActorRef subscriberRef, long leases) {
        this.subscriberRef = subscriberRef;
        this.leases = leases;
    }

    public ActorRef getSubscriberRef() {
        return subscriberRef;
    }

    public long getLeases() {
        return leases;
    }

    public long incrementAndGet(long inc) {
        if(inc == Long.MAX_VALUE) {
            leases = Long.MAX_VALUE;
        } else {
            try {
                leases = addExact(leases, inc);
            } catch(ArithmeticException e) {
                // set to max value
                leases = Long.MAX_VALUE;
            }
        }
        return leases;
    }

    public long getAndDecrement() {
        // never go below 0
        if(leases > 0) {
            leases -= 1;
            return leases+1;
        } else {
            return 0;
        }

    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MessageSubscriber that = (MessageSubscriber) o;

        return subscriberRef.equals(that.subscriberRef);
    }

    @Override
    public int hashCode() {
        return subscriberRef.hashCode();
    }
}
