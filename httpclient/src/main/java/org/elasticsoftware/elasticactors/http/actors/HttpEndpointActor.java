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

package org.elasticsoftware.elasticactors.http.actors;

import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.TempActor;
import org.elasticsoftware.elasticactors.TypedActor;
import org.elasticsoftware.elasticactors.UnexpectedResponseTypeException;
import org.elasticsoftware.elasticactors.http.messages.HttpErrorMessage;
import org.elasticsoftware.elasticactors.http.messages.HttpExceptionMessage;
import org.elasticsoftware.elasticactors.http.messages.HttpSuccessMessage;

import java.util.concurrent.CompletableFuture;

/**
 * @author Joost van de Wijgerd
 */
@TempActor(stateClass = HttpEndpointState.class)
public final class HttpEndpointActor extends TypedActor<Object> {

    @Override
    public void onReceive(ActorRef sender, Object httpMessage) throws Exception {
        CompletableFuture completableFuture = getState(HttpEndpointState.class).getFuture();
        if(httpMessage instanceof HttpSuccessMessage) {
            Object message = ((HttpSuccessMessage)httpMessage).getResponseBody();
            if(getState(HttpEndpointState.class).getResponseType().isInstance(message)) {
                completableFuture.complete(message);
            } else {
                completableFuture.completeExceptionally(new UnexpectedResponseTypeException(""));
            }
        } else if(httpMessage instanceof HttpExceptionMessage) {
            completableFuture.completeExceptionally(((HttpExceptionMessage)httpMessage).getThrowable());
        } else if(httpMessage instanceof HttpErrorMessage) {

        }
        getSystem().stop(getSelf());
    }
}
