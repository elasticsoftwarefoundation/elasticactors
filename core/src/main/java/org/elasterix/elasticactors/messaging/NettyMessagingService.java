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

package org.elasterix.elasticactors.messaging;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.*;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.net.InetSocketAddress;
import java.net.SocketAddress;

/**
 * @author Joost van de Wijgerd
 */
public class NettyMessagingService implements ChannelPipelineFactory {

    public static final String HANDLER = "handler";

    private final ChannelFactory channelFactory;
    private final int listenPort;

    private int socketBacklog = 128;
    private boolean socketReuseAddress = true;
    private int childSocketKeepAlive = 10;
    private boolean childSocketTcpNoDelay = true;
    private int childSocketReceiveBufferSize = 8192;
    private int childSocketSendBufferSize = 8192;

    private Channel serverChannel;

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
        //@todo: add encoder and decoder
        //@todo: imlement handler
        //pipeline.addLast(HANDLER, socketConnectorHandler);
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

    public void setServerChannel(Channel serverChannel) {
        this.serverChannel = serverChannel;
    }
}
