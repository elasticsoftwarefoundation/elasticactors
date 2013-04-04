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

package org.elasticsoftwarefoundation.elasticactors.http.actors;

import org.elasterix.elasticactors.ActorRef;
import org.elasterix.elasticactors.TypedActor;
import org.elasticsoftwarefoundation.elasticactors.http.messages.HttpResponse;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import static org.jboss.netty.handler.codec.http.HttpVersion.HTTP_1_1;

/**
 * @author Joost van de Wijgerd
 */
public class HttpServiceResponseHandler extends TypedActor<HttpResponse> {
    public static final class State {
        private transient final Channel responseChannel;

        public State(Channel responseChannel) {
            this.responseChannel = responseChannel;
        }

        public Channel getResponseChannel() {
            return responseChannel;
        }
    }

    @Override
    public void onReceive(HttpResponse message, ActorRef sender) throws Exception {
        State state = getState(null).getAsObject(State.class);
        // convert back to netty http response
        org.jboss.netty.handler.codec.http.HttpResponse nettyResponse = new DefaultHttpResponse(HTTP_1_1, HttpResponseStatus.valueOf(message.getStatusCode()));
        HttpHeaders.setHeader(nettyResponse,HttpHeaders.Names.CONTENT_TYPE,message.getContentType());
        nettyResponse.setContent(ChannelBuffers.wrappedBuffer(message.getResponseBody()));
        // we don't support keep alive as of yet
        state.getResponseChannel().write(nettyResponse).addListener(ChannelFutureListener.CLOSE);
        // need to remove myself
        // @todo: add a stop/destroy method
    }
}
