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

import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.ListMultimap;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.BoundRequestBuilder;
import org.elasticsoftware.elasticactors.EndpointRef;
import org.elasticsoftware.elasticactors.HttpService;
import org.elasticsoftware.elasticactors.cluster.InternalActorSystem;

/**
 * @author Joost van de Wijgerd
 */
public final class HttpServiceInstance implements HttpService {
    private final InternalActorSystem actorSystem;
    private final AsyncHttpClient asyncHttpClient;
    private final String name;
    private final String baseUrl;

    public HttpServiceInstance(InternalActorSystem actorSystem, AsyncHttpClient asyncHttpClient,
                               String name, String baseUrl) {
        this.actorSystem = actorSystem;
        this.asyncHttpClient = asyncHttpClient;
        this.name = name;
        this.baseUrl = baseUrl;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public EndpointRef endpointFor(HttpMethod method, String path, Headers headers) {
        return new HttpEndpointRef(actorSystem, this, method, path, ((HeadersBuilder)headers).build());
    }

    @Override
    public Headers newHeaders() {
        return new HeadersBuilder();
    }

    private final class HeadersBuilder implements Headers {
        private final ListMultimap<String, String> headers = LinkedListMultimap.create();

        @Override
        public Headers addHeader(String name, String value) {
            headers.put(name, value);
            return this;
        }

        protected ListMultimap<String, String> build() {
            return headers;
        }
    }

    protected BoundRequestBuilder prepare(HttpMethod method, String path) {
        switch (method) {
            case DELETE:
                return asyncHttpClient.prepareDelete(baseUrl+path);
            case GET:
                return asyncHttpClient.prepareGet(baseUrl+path);
            case HEAD:
                return asyncHttpClient.prepareHead(baseUrl+path);
            case OPTIONS:
                return asyncHttpClient.prepareOptions(baseUrl+path);
            case PATCH:
                return asyncHttpClient.preparePatch(baseUrl+path);
            case POST:
                return asyncHttpClient.preparePost(baseUrl+path);
            case PUT:
                return asyncHttpClient.preparePut(baseUrl+path);
            case TRACE:
                return asyncHttpClient.prepareTrace(baseUrl+path);
            default:
                throw new IllegalArgumentException("Unknown HTTP Method");
        }
    }
}
