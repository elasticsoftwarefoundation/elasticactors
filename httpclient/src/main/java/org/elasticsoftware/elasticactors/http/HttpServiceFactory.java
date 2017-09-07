package org.elasticsoftware.elasticactors.http;

import org.asynchttpclient.AsyncHttpClient;
import org.elasticsoftware.elasticactors.HttpService;
import org.elasticsoftware.elasticactors.cluster.InternalActorSystem;

public final class HttpServiceFactory {
    private final InternalActorSystem internalActorSystem;
    private final AsyncHttpClient asyncHttpClient;

    public HttpServiceFactory(InternalActorSystem internalActorSystem, AsyncHttpClient asyncHttpClient) {
        this.internalActorSystem = internalActorSystem;
        this.asyncHttpClient = asyncHttpClient;
    }

    public HttpService createService(String name, String baseUrl) {
        return new HttpServiceInstance(internalActorSystem, asyncHttpClient, name, baseUrl);
    }
}
