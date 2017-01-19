/*
 * Copyright 2013 - 2016 The Original Authors
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

import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.TempActor;
import org.elasticsoftware.elasticactors.TypedActor;
import org.elasticsoftware.elasticactors.base.actors.ActorDelegate;

/**
 * @author Joost van de Wijgerd
 */
@TempActor(stateClass = SubscriberActorDelegate.class)
public final class SubscriberActor<T> extends TypedActor<T> {
    @Override
    public void onUndeliverable(ActorRef receiver, Object message) throws Exception {
        final SubscriberActorDelegate delegate = getState(SubscriberActorDelegate.class);
        delegate.onUndeliverable(receiver,message);
    }

    @Override
    public void onReceive(ActorRef sender, T message) throws Exception {
        final SubscriberActorDelegate delegate = getState(SubscriberActorDelegate.class);
        delegate.onReceive(sender, message);
    }
}
