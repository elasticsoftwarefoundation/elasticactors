/*
 * Copyright 2013 - 2025 The Original Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */

package org.elasticsoftware.elasticactors.rabbitmq;

import com.rabbitmq.client.Channel;
import org.elasticsoftware.elasticactors.rabbitmq.ack.BufferingMessageAcker;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.booleanThat;
import static org.mockito.ArgumentMatchers.longThat;
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

        Thread.sleep(1000);

        verifyNoInteractions(channel);

        for(long i = 1; i < 100; i++) {
            messageAcker.deliver(i);
        }

        Thread.sleep(1000);

        verifyNoInteractions(channel);

        // ack the first 99 but not the first (nothing should be acked)
        for(long i = 2; i < 100; i++) {
            messageAcker.ack(i);
        }

        Thread.sleep(1000);

        verifyNoInteractions(channel);

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

        verify(channel, timeout(1000)).basicAck(999, true);

        // In case the for-loop above ran slower than the polling inside the Acker
        verify(channel, atMost(999 - 103)).basicAck(longThat(i -> i >= 103 && i < 999), booleanThat(Boolean::booleanValue));

        // deliver one more message

        messageAcker.deliver(1000);

        Thread.sleep(1000);

        verifyNoMoreInteractions(channel);

        messageAcker.stop();
    }
}
