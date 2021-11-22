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

import org.elasticsoftware.elasticactors.ActorNotFoundException;
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.UnexpectedResponseTypeException;
import org.elasticsoftware.elasticactors.concurrent.ActorCompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.String.format;

/**
 * @author Joost van de Wijgerd
 */
public final class CompletableFutureDelegate<T> extends InternalActorDelegate<T> {

    private static final Logger staticLogger = LoggerFactory.getLogger(CompletableFutureDelegate.class);

    private final ActorCompletableFuture<T> future;
    private final Class<T> responseType;

    public CompletableFutureDelegate(
        ActorCompletableFuture<T> future,
        Class<T> responseType,
        ActorRef callerRef)
    {
        super(true, callerRef, TEMP_ACTOR_TIMEOUT_MAX);
        this.future = future;
        this.responseType = responseType;
    }

    @Override
    protected Logger initLogger() {
        return staticLogger;
    }

    @Override
    public void onUndeliverable(ActorRef receiver, Object message) {
        future.completeExceptionally(new ActorNotFoundException(format("Actor with id [%s] does not exist",receiver.getActorId()), receiver));
    }

    @Override
    public void onReceive(ActorRef sender, Object message) {
        if (responseType.isInstance(message)) {
            future.complete((T) message);
        } else if (message instanceof Throwable) {
            future.completeExceptionally((Throwable) message);
        } else {
            future.completeExceptionally(new UnexpectedResponseTypeException(String.format(
                "Receiver unexpectedly responded with a message of type [%s]",
                message.getClass().getTypeName()
            )));
        }
    }
}
