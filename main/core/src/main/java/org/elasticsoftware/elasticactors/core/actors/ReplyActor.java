/*
 *   Copyright 2013 - 2019 The Original Authors
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

package org.elasticsoftware.elasticactors.core.actors;

import org.elasticsoftware.elasticactors.ActorContainer;
import org.elasticsoftware.elasticactors.ActorContainerRef;
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.TempActor;
import org.elasticsoftware.elasticactors.TypedActor;
import org.elasticsoftware.elasticactors.messaging.internal.PersistActorMessage;

/**
 * @author Joost van de Wijgerd
 */
@TempActor(stateClass = ActorDelegate.class)
public final class ReplyActor<T> extends TypedActor<T> {
    @Override
    public void onUndeliverable(ActorRef receiver, Object message) throws Exception {
        final ActorDelegate delegate = getState(ActorDelegate.class);
        delegate.onUndeliverable(receiver,message);
        if(delegate.isDeleteAfterReceive()) {
            getSystem().stop(getSelf());
        }
    }

    @Override
    public void onReceive(ActorRef sender, T message) throws Exception {
        final ActorDelegate delegate = getState(ActorDelegate.class);
        delegate.onReceive(sender, message);
        if(delegate.getCallerRef() != null) {
            if(delegate.getCallerRef() instanceof ActorContainerRef) {
                ActorContainer shard = ((ActorContainerRef)delegate.getCallerRef()).getActorContainer();
                shard.sendMessage(null, shard.getActorRef(), new PersistActorMessage(delegate.getCallerRef()));
            }
        }
        if(delegate.isDeleteAfterReceive()) {
            getSystem().stop(getSelf());
        }
    }
}
