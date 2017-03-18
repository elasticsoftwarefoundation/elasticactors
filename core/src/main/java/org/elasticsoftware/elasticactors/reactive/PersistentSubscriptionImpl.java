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

package org.elasticsoftware.elasticactors.reactive;

import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.PersistentSubscription;

/**
 * @author Joost van de Wijgerd
 */
public final class PersistentSubscriptionImpl implements PersistentSubscription {
    private final ActorRef publisherRef;
    private final String messageName;

    public PersistentSubscriptionImpl(ActorRef publisherRef, String messageName) {
        this.publisherRef = publisherRef;
        this.messageName = messageName;
    }

    @Override
    public ActorRef getPublisherRef() {
        return publisherRef;
    }

    @Override
    public String getMessageName() {
        return messageName;
    }

    @Override
    public void request(long n) {

    }

    @Override
    public void cancel() {

    }
}
