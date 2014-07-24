package org.elasticsoftware.elasticactors.rabbitmq;

import com.rabbitmq.client.Channel;
import org.apache.log4j.Logger;
import org.elasticsoftware.elasticactors.util.concurrent.DaemonThreadFactory;

import java.io.IOException;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;

/**
 * @author Joost van de Wijgerd
 */
public final class BufferingMessageAcker implements Runnable, MessageAcker {
    private static final Logger logger = Logger.getLogger(BufferingMessageAcker.class);
    private final Channel consumerChannel;
    private final LinkedBlockingQueue<Tag> tagQueue = new LinkedBlockingQueue<>();
    private long lastAckedTag = -1;
    private long highestDeliveredTag = -1;
    private final TreeSet<Long> pendingTags = new TreeSet<>();
    //private final ArrayDeque<Long> pendingTags = new ArrayDeque<>(1024);
    private final long maxWaitMillis = 1000;
    private final ThreadFactory threadFactory;
    private final CountDownLatch shutdownLatch = new CountDownLatch(1);

    public BufferingMessageAcker(Channel consumerChannel) {
        this.consumerChannel = consumerChannel;
        this.threadFactory = new DaemonThreadFactory("RABBITMQ-MESSAGE_ACKER");
    }

    @Override
    public void deliver(long deliveryTag) {
        tagQueue.offer(new Tag(TagType.DELIVERED,deliveryTag));
    }

    @Override
    public void ack(long deliveryTag) {
        tagQueue.offer(new Tag(TagType.ACK,deliveryTag));
    }

    @Override
    public void start() {
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
        long startTime = System.currentTimeMillis();
        while(true) {
            try {
                long elapsedTime = System.currentTimeMillis() - startTime;
                // there is still time
                if(elapsedTime < maxWaitMillis) {
                    Tag tag = tagQueue.poll(maxWaitMillis - elapsedTime,TimeUnit.MILLISECONDS);
                    if(tag == null) {
                        // timeout fired, flush ack and reset the start time
                        flushAck();
                        startTime = System.currentTimeMillis();
                    } else if(tag.type == TagType.DELIVERED) {
                        // register the last delivered tag
                        highestDeliveredTag = (tag.value > highestDeliveredTag) ? tag.value : highestDeliveredTag;
                        // add to the end of the list (in order)
                        pendingTags.add(tag.value);
                    } else if(tag.type == TagType.ACK) {
                        // search from the beginning and delete
                        pendingTags.remove(tag.value);
                    } else if(tag.type == TagType.RESET) {
                        // not sure what to do here
                    } else if(tag.type == TagType.STOP) {
                        // flush first
                        flushAck();
                        shutdownLatch.countDown();
                        break;
                    }
                } else {
                    // time is elapsed, flush and reset the start time
                    flushAck();
                    startTime = System.currentTimeMillis();
                }
            } catch(Throwable t) {
                logger.warn("Caught Throwable",t);
            }
        }
    }

    private void flushAck() {
        // we should only send an ack if something has change
        if(lastAckedTag == highestDeliveredTag) {
            return;
        }
        // get the head of the deque
        Long lowestUnAckedTag = (pendingTags.isEmpty()) ? null : pendingTags.first();
        long ackUntil = (lowestUnAckedTag != null) ? lowestUnAckedTag - 1 : highestDeliveredTag;
        // don't ack 0 as it will ack all pending messages!!!
        if(ackUntil > 0) {
            try {
                consumerChannel.basicAck(ackUntil,true);
                if(logger.isInfoEnabled()) {
                    logger.info(format("Acked all messages from %d up until %d",lastAckedTag,ackUntil));
                }
                lastAckedTag = ackUntil;
            } catch (IOException e) {
                logger.error("Exception while acking message", e);
            }
        }
    }

    private static enum TagType {
        DELIVERED,ACK,RESET,STOP
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
