package org.elasticsoftware.elasticactors.rabbitmq;

import com.rabbitmq.client.Channel;
import net.jodah.lyra.event.ChannelListener;

/**
 * @author Joost van de Wijgerd
 */
public interface ChannelListenerRegistry {
    void addChannelListener(Channel channel,ChannelListener channelListener);

    void removeChannelListener(Channel channel, ChannelListener channelListener);
}
