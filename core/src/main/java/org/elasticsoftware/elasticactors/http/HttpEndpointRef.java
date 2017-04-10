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

package org.elasticsoftware.elasticactors.http;

import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimaps;
import org.asynchttpclient.BoundRequestBuilder;
import org.asynchttpclient.Response;
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.EndpointRef;
import org.elasticsoftware.elasticactors.HttpService;
import org.elasticsoftware.elasticactors.cluster.InternalActorSystem;
import org.elasticsoftware.elasticactors.core.actors.CompletableFutureDelegate;
import org.elasticsoftware.elasticactors.core.actors.ReplyActor;
import org.elasticsoftware.elasticactors.messaging.http.HttpErrorMessage;
import org.elasticsoftware.elasticactors.messaging.http.HttpExceptionMessage;
import org.elasticsoftware.elasticactors.messaging.http.HttpSuccessMessage;
import org.elasticsoftware.elasticactors.serialization.MessageSerializer;
import org.springframework.http.HttpHeaders;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * @author Joost van de Wijgerd
 */
public final class HttpEndpointRef implements EndpointRef {
    private final InternalActorSystem actorSystem;
    private final HttpServiceInstance httpService;
    private final HttpService.HttpMethod method;
    private final String path;
    private final ListMultimap<String, String> headers;

    public HttpEndpointRef(InternalActorSystem actorSystem, HttpServiceInstance httpService,
                           HttpService.HttpMethod method, String path, ListMultimap<String, String> headers) {
        this.actorSystem = actorSystem;
        this.httpService = httpService;
        this.method = method;
        this.path = path;
        this.headers = headers;
    }

    @Override
    public HttpService getService() {
        return httpService;
    }

    @Override
    public String getPath() {
        return path;
    }

    @Override
    public Map<String, List<String>> getHeaders() {
        return Multimaps.asMap(headers);
    }

    @Override
    public <T> CompletionStage<T> call(Object message, Class<T> responseType) {
        // make sure we have a serializer and a deserializer for this class
        if(actorSystem.getDeserializer(responseType) == null) {
            throw new IllegalArgumentException("No MessageDeserializer found for responseType" +
                    "Make sure the class is annotated with the @Message annotation and that it's picked up by the messages scanner");
        }
        MessageSerializer messageSerializer = actorSystem.getSerializer(message.getClass());
        if(messageSerializer == null) {
            throw new IllegalArgumentException("No MessageSerializer found for message. " +
                    "Make sure the class is annotated with the @Message annotation and that it's picked up by the messages scanner");
        }
        final CompletableFuture<T> future = new CompletableFuture<>();

        try {
            ByteBuffer messageBytes = messageSerializer.serialize(message);
            BoundRequestBuilder requestBuilder = httpService.prepare(method, path);
            requestBuilder.setHeaders(Multimaps.asMap(headers));
            // make sure we set the correct content type header
            requestBuilder.setHeader(HttpHeaders.CONTENT_TYPE, messageSerializer.getContentType());
            // and also for the return type
            requestBuilder.setHeader(HttpHeaders.ACCEPT, actorSystem.getDeserializer(responseType).getAcceptedContentType());
            requestBuilder.setBody(messageBytes);

            // the future will need to be completed from the TempActor thread that will run on the same
            // actor thread when this is called from within a actor context

            ActorRef replyRef = actorSystem.tempActorOf(ReplyActor.class, new CompletableFutureDelegate<>(future, responseType));

            requestBuilder.execute().toCompletableFuture()
                    .exceptionally(throwable -> handleException(throwable, replyRef))
                    .thenApply(response -> handleResponse(response, replyRef, responseType));
        } catch(Exception e) {
            future.completeExceptionally(e);
        }

        return future;
    }

    @Nullable
    private Response handleException(Throwable throwable, ActorRef endpointRef) {
        endpointRef.tell(new HttpExceptionMessage(throwable), null);
        return null;
    }

    @Nullable
    private Void handleResponse(Response response, ActorRef endpointRef, Class responseType) {
        if(response.getStatusCode() >= 200 && response.getStatusCode() < 300) {
            if(response.hasResponseBody()) {
                try {
                    Object message = actorSystem.getDeserializer(responseType).deserialize(response.getResponseBodyAsByteBuffer());
                    endpointRef.tell(new HttpSuccessMessage(response.getStatusCode(), message));
                } catch(Exception e) {
                    endpointRef.tell(new HttpExceptionMessage(e), null);
                }
            } else {
                endpointRef.tell(new HttpSuccessMessage(response.getStatusCode()));
            }
        } else {
            endpointRef.tell(new HttpErrorMessage(response.getStatusCode()));
        }
        return null;
    }
}
