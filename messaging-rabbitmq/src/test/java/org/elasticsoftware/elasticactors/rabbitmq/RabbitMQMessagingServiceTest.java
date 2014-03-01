/*
 * Copyright 2013 - 2014 The Original Authors
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

package org.elasticsoftware.elasticactors.rabbitmq;

import com.google.common.base.Charsets;
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.PhysicalNode;
import org.elasticsoftware.elasticactors.cluster.ActorRefFactory;
import org.elasticsoftware.elasticactors.messaging.*;
import org.elasticsoftware.elasticactors.serialization.internal.ActorRefDeserializer;
import org.elasticsoftware.elasticactors.util.concurrent.DaemonThreadFactory;
import org.elasticsoftware.elasticactors.util.concurrent.ThreadBoundExecutor;
import org.elasticsoftware.elasticactors.util.concurrent.ThreadBoundExecutorImpl;
import org.elasticsoftware.elasticactors.util.concurrent.ThreadBoundRunnable;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertTrue;

/**
 * @author Joost van de Wijgerd
 */
public class RabbitMQMessagingServiceTest {
    public final int NUM_PARTITIONS = 64;
    public final int NUM_MESSAGES = 10000;
    public final String CLUSTER_NAME = "test.vdwbv.com";
    public static final String QUEUENAME_FORMAT = "default/shards/%d";
    public static final String PAYLOAD_FORMAT = "This is message numero %d from %s";
    public final Random random = new Random();
    private ActorRef senderRef;
    private ActorRef receiverRef;

    @BeforeTest(alwaysRun = true)
    public void setUp() {
        senderRef = mock(ActorRef.class);
        receiverRef = mock(ActorRef.class);

        ActorRefFactory actorRefFactory = mock(ActorRefFactory.class);

        when(receiverRef.toString()).thenReturn("actor://test.vdwbv.com/test/shards/1/testReceiver");
        when(senderRef.toString()).thenReturn("actor://test.vdwbv.com/test/shards/1/testSender");

        when(actorRefFactory.create("actor://test.vdwbv.com/test/shards/1/testReceiver")).thenReturn(receiverRef);
        when(actorRefFactory.create("actor://test.vdwbv.com/test/shards/1/testSender")).thenReturn(senderRef);

        // not a very nice construction, but alas
        ActorRefDeserializer.get().setActorRefFactory(actorRefFactory);
    }

    @Test
    public void testAllLocal() throws Exception {
        int workers = Runtime.getRuntime().availableProcessors() * 3;
        ThreadBoundExecutor<String> queueExecutor = new ThreadBoundExecutorImpl(new DaemonThreadFactory("QUEUE-WORKER"),workers);

        RabbitMQMessagingService messagingService = new RabbitMQMessagingService(CLUSTER_NAME,System.getProperty("host","localhost"), queueExecutor);
        messagingService.start();

        final CountDownLatch waitLatch = new CountDownLatch(NUM_MESSAGES);

        MessageHandler testHandler = new MessageHandler() {
            @Override
            public PhysicalNode getPhysicalNode() {
                return null;
            }

            @Override
            public void handleMessage(InternalMessage message, MessageHandlerEventListener messageHandlerEventListener) {
                /*byte[] buffer = new byte[message.getPayload().remaining()];
                message.getPayload().get(buffer);
                System.out.println(new String(buffer,Charsets.UTF_8));*/
                messageHandlerEventListener.onDone(message);
                waitLatch.countDown();
            }
        };

        List<MessageQueue> messageQueues = new LinkedList<>();
        // simulate 8 partitions
        MessageQueueFactory localMessageQueueFactory = messagingService.getLocalMessageQueueFactory();
        for (int i = 0; i < NUM_PARTITIONS; i++) {
            messageQueues.add(localMessageQueueFactory.create(format(QUEUENAME_FORMAT, i),testHandler));
        }

        int NUMBER_OF_THREADS = 4;

        for (int i = 0; i < NUMBER_OF_THREADS; i++) {
            Thread t = new Thread(new MessageSender(NUM_MESSAGES/NUMBER_OF_THREADS,messageQueues),format("PRODUCER #%s",i));
            t.setDaemon(true);
            t.start();
        }

        try {
            assertTrue(waitLatch.await(60, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            // ignore
        }

        for (MessageQueue messageQueue : messageQueues) {
            messageQueue.destroy();
        }

        messagingService.stop();
    }

    private final class MessageSender implements ThreadBoundRunnable<String> {
        private final Integer messagesToSend;
        private final List<MessageQueue> messageQueues;

        private MessageSender(Integer messagesToSend, List<MessageQueue> messageQueues) {
            this.messagesToSend = messagesToSend;
            this.messageQueues = messageQueues;
        }

        @Override
        public String getKey() {
            return messagesToSend.toString();
        }

        @Override
        public void run() {
            final String name = Thread.currentThread().getName();
            // send the messages
            for (int i = 0; i < messagesToSend ; i++) {
                // select a random queue
                messageQueues.get(random.nextInt(NUM_PARTITIONS)).offer(createInternalMessage(name,i+1));
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    // ignore
                }
            }

        }
    }

    private InternalMessage createInternalMessage(String name,int count) {
        ByteBuffer payload = ByteBuffer.wrap(format(PAYLOAD_FORMAT, count, name).getBytes(Charsets.UTF_8));
        return new InternalMessageImpl(senderRef,receiverRef,payload,String.class.getName(),true);
    }
}
