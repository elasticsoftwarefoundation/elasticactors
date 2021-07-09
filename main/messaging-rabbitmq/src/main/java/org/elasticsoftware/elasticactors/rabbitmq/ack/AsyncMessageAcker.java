/*
 *   Copyright 2013 - 2019 The Original Authors
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

import com.rabbitmq.client.Channel;
import org.elasticsoftware.elasticactors.rabbitmq.MessageAcker;
import org.elasticsoftware.elasticactors.util.concurrent.DaemonThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.Executors.newSingleThreadExecutor;

/**
 * @author Joost van de Wijgerd
 */
public final class AsyncMessageAcker implements MessageAcker {
    private static final Logger logger = LoggerFactory.getLogger(AsyncMessageAcker.class);
    private final ExecutorService executorService = newSingleThreadExecutor(new DaemonThreadFactory("RABBITMQ-MESSAGE_ACKER"));
    private final Channel consumerChannel;

    public AsyncMessageAcker(Channel consumerChannel) {
        this.consumerChannel = consumerChannel;
    }

    @Override
    public void deliver(long deliveryTag) {

    }

    @Override
    public void ack(long deliveryTag) {
        try {
            executorService.execute(new AckingRunnable(deliveryTag));
        } catch(RejectedExecutionException e) {
            // it's an unbounded queue so this will only happen on shutdown
        }
    }

    @Override
    public void start() {

    }

    @Override
    public void stop() {
        executorService.shutdown();
        try {
            executorService.awaitTermination(3, TimeUnit.SECONDS);
        } catch(InterruptedException e) {
            // ignore
        }
    }

    private final class AckingRunnable implements Runnable {
        private final long deliveryTag;

        private AckingRunnable(long deliveryTag) {
            this.deliveryTag = deliveryTag;
        }

        @Override
        public void run() {
            try {
                consumerChannel.basicAck(this.deliveryTag,false);
            } catch (Exception e) {
                logger.error("Unexpected Exception while acking message [{}]",deliveryTag,e);
            }
        }
    }
}
