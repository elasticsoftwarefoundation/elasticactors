package org.elasticsoftware.elasticactors.configuration;

import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.AsyncHttpClientConfig;
import org.asynchttpclient.DefaultAsyncHttpClient;
import org.asynchttpclient.DefaultAsyncHttpClientConfig;
import org.elasticsoftware.elasticactors.cluster.InternalActorSystem;
import org.elasticsoftware.elasticactors.http.HttpServiceFactory;
import org.springframework.context.annotation.Bean;

public class HttpClientConfiguration {
    @Bean(name = "asyncHttpClient")
    AsyncHttpClient createAsyncHttpClient() {
        AsyncHttpClientConfig config = new DefaultAsyncHttpClientConfig.Builder()
                .setIoThreadsCount(1)
                .setKeepAlive(true)
                .setTcpNoDelay(true).build();
        return new DefaultAsyncHttpClient(config);
    }

    @Bean(name = "httpServiceFactory")
    HttpServiceFactory createHttpServiceFactory(InternalActorSystem actorSystem, AsyncHttpClient asyncHttpClient) {
        return new HttpServiceFactory(actorSystem, asyncHttpClient);
    }
}
