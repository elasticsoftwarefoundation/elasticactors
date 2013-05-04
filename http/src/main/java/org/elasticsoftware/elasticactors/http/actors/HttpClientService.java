/*
 * Copyright 2013 Joost van de Wijgerd
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

import com.ning.http.client.AsyncCompletionHandler;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.Response;
import org.apache.log4j.Logger;
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.TypedActor;
import org.elasticsoftware.elasticactors.http.messages.HttpRequest;
import org.elasticsoftware.elasticactors.http.messages.HttpResponse;

import java.util.List;
import java.util.Map;

/**
 * @author Joost van de Wijgerd
 */
public final class HttpClientService extends TypedActor<HttpRequest> {
    private static final Logger logger = Logger.getLogger(HttpClientService.class);
    private final AsyncHttpClient httpClient;

    public HttpClientService(AsyncHttpClient httpClient) {
        this.httpClient = httpClient;
    }

    @Override
    public void onReceive(ActorRef sender, HttpRequest message) throws Exception {
        // run the request via ning and return the response async
        httpClient.prepareGet(message.getUrl()).execute(new ResponseHandler(getSelf(),sender));
    }

    private static final class ResponseHandler extends AsyncCompletionHandler<Integer> {
        private final ActorRef serviceAddress;
        private final ActorRef replyAddress;

        private ResponseHandler(ActorRef serviceAddress, ActorRef replyAddress) {
            this.serviceAddress = serviceAddress;
            this.replyAddress = replyAddress;
        }

        @Override
        public Integer onCompleted(Response response) throws Exception {
            // get the headers

            Map<String,List<String>> headers = response.getHeaders();
            replyAddress.tell(new HttpResponse(response.getStatusCode(),
                                               headers,
                                               response.getResponseBodyAsBytes()),serviceAddress);
            return response.getStatusCode();
        }

        @Override
        public void onThrowable(Throwable t) {
            // @todo: send message back to replyAddress
            logger.error("Exception getting response",t);
        }
    }
}
