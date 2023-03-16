/*
 * Copyright 2013 - 2023 The Original Authors
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

package org.elasticsoftware.elasticactors.rabbitmq.ack;

import com.rabbitmq.client.Channel;
import org.elasticsoftware.elasticactors.rabbitmq.MessageAcker;
import org.elasticsoftware.elasticactors.util.concurrent.DaemonThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.MICROSECONDS;

/**
 * @author Joost van de Wijgerd
 */
public final class BufferingMessageAcker implements Runnable, MessageAcker {
    private static final Logger logger = LoggerFactory.getLogger(BufferingMessageAcker.class);
    private final Channel consumerChannel;
    private final LinkedBlockingQueue<Tag> tagQueue = new LinkedBlockingQueue<>();
    private long lastAckedTag = -1;
    private long highestAckedTag = -1;
    private final TreeSet<Long> pendingTags = new TreeSet<>();
    private final ThreadFactory threadFactory;
    private final CountDownLatch shutdownLatch = new CountDownLatch(1);

    public BufferingMessageAcker(Channel consumerChannel) {
        this.consumerChannel = consumerChannel;
        this.threadFactory = new DaemonThreadFactory("RABBITMQ-MESSAGE-ACKER");
    }

    @Override
    public void deliver(long deliveryTag) {
        // Nothing to do here
    }

    @Override
    public void ack(long deliveryTag) {
        tagQueue.offer(new Tag(TagType.ACK,deliveryTag));
    }

    @Override
    public void start() {
        logger.info("Using MessageAcker [{}]", getClass().getSimpleName());
        final Thread t = threadFactory.newThread(this);
        t.start();
    }

    @Override
    public void stop() {
        tagQueue.offer(new Tag(TagType.STOP,-1));
        try {
            shutdownLatch.await(1,TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            // ignore
        }
    }

    @Override
    public void run() {
        while(true) {
            try {
                // poll with a short wait time (@todo: this could be dangerous)
                Tag tag = tagQueue.poll(200, MICROSECONDS);
                // need to trigger the flush
                if(tag == null) {
                    flushAck();
                } else if(tag.type == TagType.ACK) {
                    // search from the beginning and delete
                    pendingTags.add(tag.value);
                    // register the last acked tag
                    highestAckedTag = Math.max(tag.value, highestAckedTag);
                } else if(tag.type == TagType.STOP) {
                    // flush first
                    flushAck();
                    shutdownLatch.countDown();
                    break;
                }
            } catch(Throwable t) {
                logger.warn("Caught Throwable",t);
            }
        }
    }

    private void flushAck() {
        // we should only send an ack if something has change
        if (lastAckedTag == highestAckedTag) {
            return;
        }
        // get the head of the deque
        Long ackUntil = pendingTags.isEmpty() ? null : pendingTags.first();
        if (ackUntil == null) {
            // no tags to ack yet
            return;
        }
        if (lastAckedTag == -1 && ackUntil > 1) {
            // cannot ack anything if we haven't acked 1 yet
            return;
        }
        if (lastAckedTag > 0 && ackUntil > lastAckedTag + 1) {
            // not ready to ack yet because it's not consecutive with what has been acked so far
            return;
        }
        // remove ackUntil
        pendingTags.pollFirst();
        // find highest tag we can ack
        Long next;
        while ((next = pendingTags.pollFirst()) != null) {
            if (next == ackUntil + 1) {
                ackUntil = next;
            } else {
                // re-add it because it's not consecutive, but we already removed it
                pendingTags.add(next);
                break;
            }
        }
        // don't ack 0 as it will ack all pending messages!!!
        if(ackUntil > 0 && ackUntil > lastAckedTag) {
            try {
                consumerChannel.basicAck(ackUntil,true);
                logger.debug("Acked all messages from {} up until {}", lastAckedTag, ackUntil);
                lastAckedTag = ackUntil;
            } catch (IOException e) {
                logger.error("Exception while acking message", e);
            }
        }
    }

    private enum TagType {
        ACK,
        STOP
    }

    private static final class Tag {
        private final TagType type;
        private final long value;

        private Tag(TagType type, long value) {
            this.type = type;
            this.value = value;
        }
    }

}
