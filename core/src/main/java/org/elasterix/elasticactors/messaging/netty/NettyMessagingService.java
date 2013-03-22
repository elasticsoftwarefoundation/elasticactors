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

package org.elasterix.elasticactors.messaging.netty;

import com.google.protobuf.Message;
import com.google.protobuf.MessageLite;
import org.elasterix.elasticactors.PhysicalNode;
import org.elasterix.elasticactors.messaging.*;
import org.elasterix.elasticactors.serialization.protobuf.Elasticactors;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.*;
import org.jboss.netty.handler.codec.frame.LengthFieldBasedFrameDecoder;
import org.jboss.netty.handler.codec.frame.LengthFieldPrepender;
import org.jboss.netty.handler.codec.protobuf.ProtobufDecoder;
import org.jboss.netty.handler.codec.protobuf.ProtobufEncoder;
import org.jboss.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import org.jboss.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.net.InetSocketAddress;
import java.net.SocketAddress;

/**
 * @author Joost van de Wijgerd
 */
public class NettyMessagingService extends SimpleChannelUpstreamHandler implements ChannelPipelineFactory, MessageQueueFactory, MessagingService {

    public static final String HANDLER = "handler";

    private final ChannelFactory channelFactory;
    private final int listenPort;

    private int socketBacklog = 128;
    private boolean socketReuseAddress = true;
    private int childSocketKeepAlive = 10;
    private boolean childSocketTcpNoDelay = true;
    private int childSocketReceiveBufferSize = 8192;
    private int childSocketSendBufferSize = 8192;

    private volatile Channel serverChannel;

    public NettyMessagingService(ChannelFactory channelFactory, int listenPort) {
        this.channelFactory = channelFactory;
        this.listenPort = listenPort;
    }

    @PostConstruct
    public void start() {
        ServerBootstrap bootstrap = new ServerBootstrap(channelFactory);
        bootstrap.setOption("backlog", socketBacklog);
        bootstrap.setOption("reuseAddress", socketReuseAddress);
        bootstrap.setOption("child.keepAlive", childSocketKeepAlive);
        bootstrap.setOption("child.tcpNoDelay", childSocketTcpNoDelay);
        bootstrap.setOption("child.receiveBufferSize", childSocketReceiveBufferSize);
        bootstrap.setOption("child.sendBufferSize", childSocketSendBufferSize);
        bootstrap.setPipelineFactory(this);

        SocketAddress addr = new InetSocketAddress(listenPort);
        serverChannel = bootstrap.bind(addr);
    }

    @PreDestroy
    public void stop() {
        serverChannel.close();
    }

    @Override
    public ChannelPipeline getPipeline() throws Exception {
       ChannelPipeline pipeline = Channels.pipeline();
        // Decoder
       pipeline.addLast("frameDecoder",new ProtobufVarint32FrameDecoder());
       pipeline.addLast("protobufDecoder",new ProtobufDecoder(Elasticactors.InternalMessage.getDefaultInstance()));
       // Encoder
       pipeline.addLast("frameEncoder", new ProtobufVarint32LengthFieldPrepender());
       pipeline.addLast("protobufEncoder", new ProtobufEncoder());
        // incoming handler
       pipeline.addLast(HANDLER, this);
       return pipeline;
    }

    public void setSocketBacklog(int socketBacklog) {
        this.socketBacklog = socketBacklog;
    }

    public void setSocketReuseAddress(boolean socketReuseAddress) {
        this.socketReuseAddress = socketReuseAddress;
    }

    public void setChildSocketKeepAlive(int childSocketKeepAlive) {
        this.childSocketKeepAlive = childSocketKeepAlive;
    }

    public void setChildSocketTcpNoDelay(boolean childSocketTcpNoDelay) {
        this.childSocketTcpNoDelay = childSocketTcpNoDelay;
    }

    public void setChildSocketReceiveBufferSize(int childSocketReceiveBufferSize) {
        this.childSocketReceiveBufferSize = childSocketReceiveBufferSize;
    }

    public void setChildSocketSendBufferSize(int childSocketSendBufferSize) {
        this.childSocketSendBufferSize = childSocketSendBufferSize;
    }

    @Override
    public MessageQueue create(String name, MessageHandler messageHandler) throws Exception {
        MessageQueue remoteMessageQueue = new RemoteMessageQueue(name,this,messageHandler);
        remoteMessageQueue.initialize();
        return remoteMessageQueue;
    }

    @Override
    public void sendWireMessage(MessageLite message, PhysicalNode receiver) {
        // find the channel for the receiver
        Channel receivingChannel = getOrCreateChannel(receiver);
        receivingChannel.write(message);
    }

    private Channel getOrCreateChannel(PhysicalNode receiver) {
        return null;
    }
}
