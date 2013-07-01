/*
 * Copyright 2013 the original authors
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
import org.elasticsoftware.elasticactors.UntypedActor;
import org.elasticsoftware.elasticactors.http.messages.HttpResponse;
import org.elasticsoftware.elasticactors.http.messages.ServerSentEvent;
import org.elasticsoftware.elasticactors.http.messages.SseResponse;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpVersion;

import static org.jboss.netty.handler.codec.http.HttpVersion.HTTP_1_1;

/**
 * @author Joost van de Wijgerd
 */
public final class HttpServiceResponseHandler extends UntypedActor {
    public static final class State {
        private transient final Channel responseChannel;
        private boolean serverSentEvents = false;

        public State(Channel responseChannel) {
            this.responseChannel = responseChannel;
        }

        public Channel getResponseChannel() {
            return responseChannel;
        }

        public boolean isServerSentEvents() {
            return serverSentEvents;
        }

        public void setServerSentEvents(boolean serverSentEvents) {
            this.serverSentEvents = serverSentEvents;
        }
    }

    @Override
    public void onReceive(ActorRef sender, Object message) throws Exception {
        if(message instanceof SseResponse) {
            handle((SseResponse) message);
        } else if(message instanceof HttpResponse) {
            handle((HttpResponse) message);
        } else if(message instanceof ServerSentEvent) {
            handle((ServerSentEvent) message);
        }
    }

    private void handle(HttpResponse message) throws Exception {
        State state = getState(null).getAsObject(State.class);
        // convert back to netty http response
        org.jboss.netty.handler.codec.http.HttpResponse nettyResponse = new DefaultHttpResponse(HTTP_1_1, HttpResponseStatus.valueOf(message.getStatusCode()));
        // @todo: copy the headers from the message
        HttpHeaders.setHeader(nettyResponse,HttpHeaders.Names.CONTENT_TYPE,message.getContentType());
        nettyResponse.setContent(ChannelBuffers.wrappedBuffer(message.getContent()));
        // we don't support keep alive as of yet
        state.getResponseChannel().write(nettyResponse).addListener(ChannelFutureListener.CLOSE);
        // need to remove myself
        getSystem().stop(getSelf());
    }

    private void handle(SseResponse message) {
        // we want to setup an EventSource connection, don't close the socket
        State state = getState(null).getAsObject(State.class);
        // register the fact we are using SSE
        state.setServerSentEvents(true);
        // send the event to the netty stack
        state.getResponseChannel().write(message);
    }

    private void handle(ServerSentEvent message) {
        State state = getState(null).getAsObject(State.class);
        // send the event to the netty stack
        state.getResponseChannel().write(message);
    }

    @Override
    public void onUndeliverable(ActorRef receiver, Object message) throws Exception {
        State state = getState(null).getAsObject(State.class);
        // send a 404
        state.getResponseChannel().write(new DefaultHttpResponse(HttpVersion.HTTP_1_1,HttpResponseStatus.NOT_FOUND)).addListener(ChannelFutureListener.CLOSE);
        // remove self
        getSystem().stop(getSelf());
    }
}
