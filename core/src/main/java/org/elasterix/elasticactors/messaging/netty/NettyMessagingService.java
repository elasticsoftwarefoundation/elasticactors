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

import com.google.protobuf.ByteString;
import com.google.protobuf.MessageLite;
import org.apache.log4j.Logger;
import org.elasterix.elasticactors.ActorSystems;
import org.elasterix.elasticactors.PhysicalNode;
import org.elasterix.elasticactors.ShardKey;
import org.elasterix.elasticactors.cluster.InternalActorSystem;
import org.elasterix.elasticactors.cluster.InternalActorSystems;
import org.elasterix.elasticactors.messaging.*;
import org.elasterix.elasticactors.serialization.internal.InternalMessageDeserializer;
import org.elasterix.elasticactors.serialization.protobuf.Elasticactors;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.socket.ClientSocketChannelFactory;
import org.jboss.netty.channel.socket.ServerSocketChannelFactory;
import org.jboss.netty.handler.codec.protobuf.ProtobufDecoder;
import org.jboss.netty.handler.codec.protobuf.ProtobufEncoder;
import org.jboss.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import org.jboss.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author Joost van de Wijgerd
 */
public final class NettyMessagingService extends SimpleChannelUpstreamHandler implements ChannelPipelineFactory, MessageQueueFactory, MessagingService {
    private static final Logger logger= Logger.getLogger(NettyMessagingService.class);
    public static final String HANDLER = "handler";

    private final ServerSocketChannelFactory serverChannelFactory;
    private final ClientSocketChannelFactory clientChannelFactory;
    private final int listenPort;
    private volatile ClientBootstrap clientBootstrap;
    private final ConcurrentMap<PhysicalNode,Channel> outgoingChannels = new ConcurrentHashMap<PhysicalNode,Channel>();

    private int socketBacklog = 128;
    private boolean socketReuseAddress = true;
    private int childSocketKeepAlive = 10;
    private boolean childSocketTcpNoDelay = true;
    private int childSocketReceiveBufferSize = 8192;
    private int childSocketSendBufferSize = 8192;

    private volatile Channel serverChannel;

    private final InternalActorSystems cluster;

    public NettyMessagingService(InternalActorSystems cluster,
                                 ServerSocketChannelFactory serverChannelFactory,
                                 ClientSocketChannelFactory clientChannelFactory,
                                 int listenPort) {
        this.serverChannelFactory = serverChannelFactory;
        this.clientChannelFactory = clientChannelFactory;
        this.listenPort = listenPort;
        this.cluster = cluster;
    }

    @PostConstruct
    public void start() {
        // start listening
        ServerBootstrap bootstrap = new ServerBootstrap(serverChannelFactory);
        bootstrap.setOption("backlog", socketBacklog);
        bootstrap.setOption("reuseAddress", socketReuseAddress);
        bootstrap.setOption("child.keepAlive", childSocketKeepAlive);
        bootstrap.setOption("child.tcpNoDelay", childSocketTcpNoDelay);
        bootstrap.setOption("child.receiveBufferSize", childSocketReceiveBufferSize);
        bootstrap.setOption("child.sendBufferSize", childSocketSendBufferSize);
        bootstrap.setPipelineFactory(this);

        SocketAddress addr = new InetSocketAddress(listenPort);
        serverChannel = bootstrap.bind(addr);
        logger.info(String.format("listening on port [%d]",listenPort));

        clientBootstrap = new ClientBootstrap(clientChannelFactory);
        clientBootstrap.setOption("keepAlive", childSocketKeepAlive);
        clientBootstrap.setOption("tcpNoDelay", childSocketTcpNoDelay);
        clientBootstrap.setOption("receiveBufferSize", childSocketReceiveBufferSize);
        clientBootstrap.setOption("sendBufferSize", childSocketSendBufferSize);
        clientBootstrap.setPipelineFactory(this);

    }

    @PreDestroy
    public void stop() {
        serverChannel.close();
        Iterator<Map.Entry<PhysicalNode,Channel>> entryIterator = outgoingChannels.entrySet().iterator();
        while(entryIterator.hasNext()) {
            entryIterator.next().getValue().close();
            entryIterator.remove();
        }
    }

    @Override
    public ChannelPipeline getPipeline() throws Exception {
       ChannelPipeline pipeline = Channels.pipeline();
        // Decoder
       pipeline.addLast("frameDecoder",new ProtobufVarint32FrameDecoder());
       pipeline.addLast("protobufDecoder",new ProtobufDecoder(Elasticactors.WireMessage.getDefaultInstance()));
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
    public void sendWireMessage(String queueName, byte[] serializedMessage, PhysicalNode receiver) {
        Elasticactors.WireMessage.Builder builder = Elasticactors.WireMessage.newBuilder();
        builder.setQueueName(queueName);
        builder.setInternalMessage(ByteString.copyFrom(serializedMessage));
        // find the channel for the receiver
        Channel receivingChannel = getOrCreateChannel(receiver);
        receivingChannel.write(builder.build());
        //logger.info(String.format("written message to %s",receiver));
    }

    private Channel getOrCreateChannel(PhysicalNode receiver) {
        // make sure we only have one active channel per remote node
        Channel outgoingChannel = outgoingChannels.get(receiver);
        if(outgoingChannel == null) {
            ChannelFuture connectFuture = clientBootstrap.connect(new InetSocketAddress(receiver.getAddress(),listenPort));
            try {
                connectFuture.await();
            } catch (InterruptedException e) {
                // ignore
            }
            if(connectFuture.isSuccess()) {
                // try to put
                outgoingChannel = connectFuture.getChannel();
                Channel existingChannel = outgoingChannels.putIfAbsent(receiver,outgoingChannel);
                if(existingChannel != null && !existingChannel.equals(outgoingChannel)) {
                    // somebody beat us to it, let's close and return existing channel
                    outgoingChannel.close();
                    return existingChannel;
                } else {
                    return outgoingChannel;
                }
            } else {
                logger.error(String.format("Unable to open Channel to [%s]",receiver.toString()));
                //@todo: throw a better exception here
                throw new RuntimeException("Exception opening Channel",connectFuture.getCause());
            }
        } else {
            return outgoingChannel;
        }
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {

        Object messageObject = e.getMessage();
        if(messageObject instanceof Elasticactors.WireMessage) {
            Elasticactors.WireMessage wireMessage = (Elasticactors.WireMessage) messageObject;
            // @todo: do this nice with some utility
            String[] pathElements = wireMessage.getQueueName().split("/");

            InternalActorSystem actorSystem = cluster.get(pathElements[0]);
            actorSystem.getShard(wireMessage.getQueueName()).offerInternalMessage(
                                 InternalMessageDeserializer.get().deserialize(wireMessage.getInternalMessage().toByteArray()));
            //logger.info(String.format("received message from %s for %s",ctx.getChannel().getRemoteAddress(),queueName));
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
        logger.error(e.getCause());
    }

    @Override
    public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {

        Iterator<Map.Entry<PhysicalNode,Channel>> iterator = outgoingChannels.entrySet().iterator();
        while(iterator.hasNext()) {
            Map.Entry<PhysicalNode,Channel> entry = iterator.next();
            if(entry.getValue().equals(e.getChannel())) {
                iterator.remove();
                break;
            }
        }
    }
}
