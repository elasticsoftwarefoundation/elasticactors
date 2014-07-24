package org.elasticsoftware.elasticactors.rabbitmq;

import com.rabbitmq.client.Channel;
import org.elasticsoftware.elasticactors.util.concurrent.DaemonThreadFactory;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;

/**
 * @author Joost van de Wijgerd
 */
public class BufferingMessageAckerTest {
    @Test
    public void testAcking() throws Exception {
        Channel channel = mock(Channel.class);

        BufferingMessageAcker messageAcker = new BufferingMessageAcker(channel);
        messageAcker.start();

        // deliver out of order
        for(long i = 100; i < 1000; i++) {
            messageAcker.deliver(i);
        }
        for(long i = 1; i < 100; i++) {
            messageAcker.deliver(i);
        }

        Thread.sleep(1000);

        verifyZeroInteractions(channel);

        // ack the first 99 but not the first (nothing should be acked)
        for(long i = 2; i < 100; i++) {
            messageAcker.ack(i);
        }

        Thread.sleep(1000);

        verifyZeroInteractions(channel);

        // now ack the first (this should cause an ack on the channel
        messageAcker.ack(1);

        verify(channel,timeout(1000)).basicAck(99, true);

        messageAcker.ack(102);
        messageAcker.ack(100);

        verify(channel,timeout(1000)).basicAck(100, true);

        messageAcker.ack(101);

        verify(channel,timeout(1000)).basicAck(102, true);

        for(long i = 103; i < 1000; i++) {
            messageAcker.ack(i);
        }

        verify(channel,timeout(1000)).basicAck(999, true);

        // deliver one more message

        messageAcker.deliver(1000);

        Thread.sleep(1000);

        verifyZeroInteractions(channel);

        messageAcker.stop();


    }
}
