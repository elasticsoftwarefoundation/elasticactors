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

package org.elasticsoftware.elasticactors.core.actors;

import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.MessageDeliveryException;
import org.elasticsoftware.elasticactors.UnexpectedResponseTypeException;

import java.util.concurrent.CompletableFuture;

/**
 * @author Joost van de Wijgerd
 */
public final class CompletableFutureDelegate<T> extends ActorDelegate<T> {
    private final CompletableFuture<T> future;
    private final Class<T> responseType;

    public CompletableFutureDelegate(CompletableFuture<T> future, Class<T> responseType) {
        this.future = future;
        this.responseType = responseType;
    }

    @Override
    public void onUndeliverable(ActorRef receiver, Object message) {
        future.completeExceptionally(new MessageDeliveryException("Unable to deliver message", false));
    }

    @Override
    public void onReceive(ActorRef sender, Object message) {
        if (responseType.isInstance(message)) {
            future.complete((T) message);
        } else if (message instanceof Throwable) {
            future.completeExceptionally((Throwable) message);
        } else {
            future.completeExceptionally(new UnexpectedResponseTypeException("Receiver unexpectedly responded with a message of type " + message.getClass().getTypeName()));
        }
    }
}
