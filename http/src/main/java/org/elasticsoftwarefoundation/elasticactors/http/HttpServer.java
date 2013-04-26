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

package org.elasticsoftwarefoundation.elasticactors.http;

import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.elasterix.elasticactors.ActorRef;
import org.elasterix.elasticactors.ActorSystem;
import org.elasticsoftwarefoundation.elasticactors.base.state.JacksonActorState;
import org.elasticsoftwarefoundation.elasticactors.http.actors.HttpService;
import org.elasticsoftwarefoundation.elasticactors.http.actors.HttpServiceResponseHandler;
import org.elasticsoftwarefoundation.elasticactors.http.codec.SseEventEncoder;
import org.elasticsoftwarefoundation.elasticactors.http.messages.HttpRequest;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.socket.ServerSocketChannelFactory;
import org.jboss.netty.handler.codec.http.*;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.jboss.netty.channel.Channels.pipeline;

/**
 * @author Joost van de Wijgerd
 */
public class HttpServer extends SimpleChannelUpstreamHandler implements ChannelPipelineFactory {
    private static final Logger logger = Logger.getLogger(HttpServer.class);
    private final SseEventEncoder sseEventEncoder = new SseEventEncoder();
    private ServerSocketChannelFactory channelFactory;
    private ActorSystem actorSystem;
    private HttpService httpService;
    private ObjectMapper objectMapper;


    @PostConstruct
    public void init() {
        ServerBootstrap bootstrap = new ServerBootstrap(channelFactory);
        bootstrap.setPipelineFactory(this);
        bootstrap.bind(new InetSocketAddress(8080));
    }

    @PreDestroy
    public void destroy() {

    }

    @Override
    public ChannelPipeline getPipeline() throws Exception {
        // Create a default pipeline implementation.
        ChannelPipeline pipeline = pipeline();

      // Uncomment the following line if you want HTTPS
      //SSLEngine engine = SecureChatSslContextFactory.getServerContext().createSSLEngine();
     //engine.setUseClientMode(false);
      //pipeline.addLast("ssl", new SslHandler(engine));

      pipeline.addLast("decoder", new HttpRequestDecoder());
      //pipeline.addLast("aggregator", new HttpChunkAggregator(65536));
      pipeline.addLast("encoder", new HttpResponseEncoder());
       pipeline.addLast("sseEventEncoder", sseEventEncoder);
      //pipeline.addLast("chunkedWriter", new ChunkedWriteHandler());

      pipeline.addLast("handler", this);
      return pipeline;
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
        org.jboss.netty.handler.codec.http.HttpRequest nettyRequest = (org.jboss.netty.handler.codec.http.HttpRequest) e.getMessage();
        // convert request to our internal request
        Map<String,List<String>> headers = new HashMap<String,List<String>>();
        for (String headerName : nettyRequest.getHeaderNames()) {
            headers.put(headerName,nettyRequest.getHeaders(headerName));
        }
        // see if we have a body
        byte[] content = null;
        if(nettyRequest.getContent().hasArray() && nettyRequest.getContent().array().length > 0) {
            content = nettyRequest.getContent().array();
        } else if(nettyRequest.getContent().readableBytes() > 0) {
            // netty content not backed by array, need to copy
            content = new byte[nettyRequest.getContent().readableBytes()];
            nettyRequest.getContent().readBytes(content);
        }
        HttpRequest request = new HttpRequest(nettyRequest.getUri(),headers,content);
        // create a temp actor to handle the response
        ActorRef replyActor = actorSystem.tempActorOf(HttpServiceResponseHandler.class,
                                                      new HttpServiceResponseHandler.State(ctx.getChannel()));
        // put the actor in the attachment to propagate disconnects
        ctx.setAttachment(replyActor);
        // async handling
        if(!httpService.doDispatch(request,replyActor)) {
            // send 404
            ctx.getChannel().write(new DefaultHttpResponse(HttpVersion.HTTP_1_1,HttpResponseStatus.NOT_FOUND)).addListener(ChannelFutureListener.CLOSE);
            actorSystem.stop(replyActor);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
        logger.error("Exception caught",e.getCause());
        ctx.getChannel().write(new DefaultHttpResponse(HttpVersion.HTTP_1_1,HttpResponseStatus.INTERNAL_SERVER_ERROR)).addListener(ChannelFutureListener.CLOSE);
    }

    @Autowired
    public void setChannelFactory(ServerSocketChannelFactory channelFactory) {
        this.channelFactory = channelFactory;
    }

    public void setActorSystem(ActorSystem actorSystem) {
        this.actorSystem = actorSystem;
    }

    @Autowired
    public void setHttpService(HttpService httpService) {
        this.httpService = httpService;
    }

    @Autowired
    public void setObjectMapper(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }
}
