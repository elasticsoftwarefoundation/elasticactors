/*
 *   Copyright 2013 - 2022 The Original Authors
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.elasticsoftware.elasticactors.rabbitmq.ack;

import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.dsl.Disruptor;
import com.rabbitmq.client.Channel;
import org.elasticsoftware.elasticactors.rabbitmq.MessageAcker;
import org.elasticsoftware.elasticactors.util.concurrent.DaemonThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Joost van de Wijgerd
 */
public final class WriteBehindMessageAcker implements MessageAcker {

    private static final Logger logger = LoggerFactory.getLogger(WriteBehindMessageAcker.class);

    private final Channel consumerChannel;
    private final AtomicBoolean shuttingDown = new AtomicBoolean(false);
    private final Disruptor<AckEvent> disruptor;
    private final DeliveryTagTranslator translator = new DeliveryTagTranslator();

    public WriteBehindMessageAcker(Channel consumerChannel) {
        this.consumerChannel = consumerChannel;
        this.disruptor = new Disruptor<>(new AckEventFactory(), 16384, new DaemonThreadFactory("RABBITMQ-MESSAGE-ACKER"));
        this.disruptor.handleEventsWith(new AckEventHandler());
    }


    @Override
    public void deliver(long deliveryTag) {
        // don't do anything
    }

    @Override
    public void ack(long deliveryTag) {
        if(!shuttingDown.get()) {
            // put the event on the RingBuffer
            this.disruptor.publishEvent(translator,deliveryTag);
        }
    }

    @Override
    public void start() {
        logger.info("Using MessageAcker [{}]", getClass().getSimpleName());
        this.disruptor.start();
    }

    @Override
    public void stop() {
        if (shuttingDown.compareAndSet(false, true)) {
            this.disruptor.shutdown();
        }
    }

    private final class AckEventHandler implements EventHandler<AckEvent> {
        @Override
        public void onEvent(AckEvent event, long sequence, boolean endOfBatch) throws Exception {
            consumerChannel.basicAck(event.deliveryTag,false);
        }
    }

    private static final class DeliveryTagTranslator implements EventTranslatorOneArg<AckEvent,Long> {
        @Override
        public void translateTo(AckEvent event, long sequence, Long deliveryTag) {
            event.deliveryTag = deliveryTag;
        }
    }

    private static final class AckEvent {
        private long deliveryTag;
    }

    private static final class AckEventFactory implements EventFactory<AckEvent> {
        @Override
        public AckEvent newInstance() {
            return new AckEvent();
        }
    }

}
