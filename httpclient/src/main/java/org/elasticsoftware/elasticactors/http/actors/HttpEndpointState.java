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

import org.elasticsoftware.elasticactors.ActorState;
import org.elasticsoftware.elasticactors.serialization.NoopSerializationFramework;
import org.elasticsoftware.elasticactors.serialization.SerializationFramework;

import java.util.concurrent.CompletableFuture;

/**
 * @author Joost van de Wijgerd
 */
public final class HttpEndpointState<T> implements ActorState<HttpEndpointState> {
    private final CompletableFuture<T> future;
    private final Class<T> responseType;

    public HttpEndpointState(CompletableFuture<T> future, Class<T> responseType) {
        this.future = future;
        this.responseType = responseType;
    }

    public CompletableFuture<T> getFuture() {
        return future;
    }

    public Class<T> getResponseType() {
        return responseType;
    }

    @Override
    public HttpEndpointState getBody() {
        return this;
    }

    @Override
    public Class<? extends SerializationFramework> getSerializationFramework() {
        return NoopSerializationFramework.class;
    }
}
