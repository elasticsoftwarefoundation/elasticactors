package org.elasticsoftwarefoundation.elasticactors.http.codec;

import com.google.common.base.Charsets;
import org.elasticsoftwarefoundation.elasticactors.http.messages.ServerSentEvent;
import org.elasticsoftwarefoundation.elasticactors.http.messages.SseResponse;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder;

import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;

import static org.jboss.netty.buffer.ChannelBuffers.dynamicBuffer;
import static org.jboss.netty.handler.codec.http.HttpConstants.*;

/**
 * @author Joost van de Wijgerd
 */
public class SseEventEncoder extends OneToOneEncoder {
    private static final Charset ASCII = Charset.forName("ASCII");
    private static final byte[] EVENT = "event".getBytes(Charsets.UTF_8);
    private static final byte[] DATA = "data".getBytes(Charsets.UTF_8);
    private static final byte[] ID = "id".getBytes(Charsets.UTF_8);

    @Override
    protected Object encode(ChannelHandlerContext ctx, Channel channel, Object msg) throws Exception {
        if(msg instanceof SseResponse) {
            SseResponse m = (SseResponse) msg;
            ChannelBuffer header = dynamicBuffer(channel.getConfig().getBufferFactory());
            encodeInitialLine(header);
            encodeHeaders(header, m);
            header.writeByte(CR);
            header.writeByte(LF);
            return header;
        } else if(msg instanceof ServerSentEvent) {
            ServerSentEvent m = (ServerSentEvent) msg;
            ChannelBuffer event = dynamicBuffer(channel.getConfig().getBufferFactory());
            if(m.getEvent() != null) {
                event.writeBytes(EVENT);
                event.writeByte(COLON);
                event.writeByte(SP);
                event.writeBytes(m.getEvent().getBytes(Charsets.UTF_8));
                event.writeByte(CR);
            }
            if(m.getData() != null && !m.getData().isEmpty()) {
                for (String data : m.getData()) {
                    event.writeBytes(DATA);
                    event.writeByte(COLON);
                    event.writeByte(SP);
                    event.writeBytes(data.getBytes(Charsets.UTF_8));
                    event.writeByte(CR);
                }
            }
            if(m.getId() != null) {
                event.writeBytes(ID);
                event.writeByte(COLON);
                event.writeByte(SP);
                event.writeBytes(m.getId().getBytes(Charsets.UTF_8));
                event.writeByte(CR);
            }
            // end with empty line
            event.writeByte(CR);
            return event;
        } else {
            return msg;
        }
    }

    private void encodeInitialLine(ChannelBuffer buf) {
        buf.writeBytes(HttpVersion.HTTP_1_1.toString().getBytes(ASCII));
        buf.writeByte(SP);
        buf.writeBytes(String.valueOf(HttpResponseStatus.OK.getCode()).getBytes(ASCII));
        buf.writeByte(SP);
        buf.writeBytes(String.valueOf(HttpResponseStatus.OK.getReasonPhrase()).getBytes(ASCII));
        buf.writeByte(CR);
        buf.writeByte(LF);
    }

    private static void encodeHeaders(ChannelBuffer buf, SseResponse message) {
        for (Map.Entry<String, List<String>> entry : message.getHeaders().entrySet()) {
            for (String value : entry.getValue()) {
                encodeHeader(buf,entry.getKey(),value);
            }
        }
    }

    private static void encodeHeader(ChannelBuffer buf, String header, String value) {
        buf.writeBytes(header.getBytes(ASCII));
        buf.writeByte(COLON);
        buf.writeByte(SP);
        buf.writeBytes(value.getBytes(ASCII));
        buf.writeByte(CR);
        buf.writeByte(LF);
    }
}
