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

package org.elasticsoftware.elasticactors.netty.messaging;

import org.apache.log4j.Logger;
import org.elasticsoftware.elasticactors.messaging.*;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author Joost van de Wijgerd
 */
public final class LocalMessageQueueWorkers implements MessageQueueFactory {
    private static final Logger LOGGER = Logger.getLogger(LocalMessageQueueWorkers.class);

    private final ExecutorService executor;
    private final int numberOfWorkers;
    private final MessageQueueWorker[] workers;
    private final AtomicInteger workerIndex = new AtomicInteger();

    private volatile boolean stop;

    public LocalMessageQueueWorkers(ExecutorService executor, int numberOfWorkers) {
        this.executor = executor;
        this.numberOfWorkers = Math.max(1, numberOfWorkers);
        workers = new MessageQueueWorker[this.numberOfWorkers];
    }

    @PostConstruct
    public void init() {
        stop = false;
        for (int i = 0; i < numberOfWorkers; i++) {
            workers[i] = new MessageQueueWorker();
            executor.submit(workers[i]);
        }
    }

    @PreDestroy
    public void destroy() {
        // @todo: this needs to be synchronized on the workers actually stopping
        stop = true;
        for (MessageQueueWorker worker : workers) {
            worker.shutdown();
        }
    }

    private MessageQueueWorker nextWorker() {
        return workers[Math.abs(workerIndex.getAndIncrement() % workers.length)];
    }

    public MessageQueue create(String name,MessageHandler messageHandler) throws Exception {
        // pick the next worker (round-robin)
        MessageQueueWorker worker = nextWorker();
        LocalMessageQueue messageQueue = new LocalMessageQueue(name,worker,messageHandler);
        // add the queue to the worker
        worker.add(messageQueue);
        // initialize the queue (will read commit log and start emitting pending messages
        messageQueue.initialize();
        return messageQueue;
    }

    private final class MessageQueueWorker implements Runnable, MessageQueueEventListener {
        private final ReentrantLock waitLock = new ReentrantLock();
        private final Condition waitCondition = waitLock.newCondition();
        private final ConcurrentMap<String,LocalMessageQueue> messageQueues;
        private final AtomicInteger pendingSignals = new AtomicInteger(0);
        private final ConcurrentMap<String,CountDownLatch> destroyLatches;

        public MessageQueueWorker() {
            messageQueues = new ConcurrentHashMap<String,LocalMessageQueue>();
            destroyLatches = new ConcurrentHashMap<String,CountDownLatch>();
        }

        public void add(LocalMessageQueue queue) {
            messageQueues.put(queue.getName(),queue);
        }

        @Override
        public void wakeUp() {
            // register the wakeUp to avoid race condition
            pendingSignals.incrementAndGet();
            // wake up the waiting worker thread
            try {
                final ReentrantLock waitLock = this.waitLock;
                waitLock.lockInterruptibly();
                try {
                    waitCondition.signal();
                } finally {
                    waitLock.unlock();
                }
            } catch (InterruptedException e) {
                // ignore
            }
        }

        @Override
        public void onDestroy(MessageQueue queue) {
            final CountDownLatch destroyLatch = new CountDownLatch(1);
            destroyLatches.put(queue.getName(),destroyLatch);
            wakeUp();
        }

        @Override
        public void run() {
            try {
                while (!stop) {
                    // first see if we have waiting queue removals
                    if(!destroyLatches.isEmpty()) {
                        for (String queueName : destroyLatches.keySet()) {
                            messageQueues.remove(queueName);
                            destroyLatches.remove(queueName).countDown();
                        }
                    }
                    // reset pending signals as we are starting the loop
                    pendingSignals.set(0);
                    boolean moreWaiting = false;
                    // make a fair loop
                    for (LocalMessageQueue messageQueue : messageQueues.values()) {
                        InternalMessage message = messageQueue.poll();
                        if (message != null) {
                            try {
                                messageQueue.getMessageHandler().handleMessage(message,
                                                                               messageQueue.getMessageHandlerEventListener());
                            } catch (Throwable e) {
                                LOGGER.error("Throwable on handleMessage!", e);
                            }
                            if(messageQueue.peek() != null) {
                                moreWaiting = true;
                            }
                        }
                    }
                    if(!moreWaiting) {
                        // all queues visited, now block until something happens
                        try {
                            final ReentrantLock waitLock = this.waitLock;
                            waitLock.lockInterruptibly();
                            try {
                                // only block if there are no pending signals
                                if(pendingSignals.get() == 0) {
                                    // @todo: very tiny race condition here!!!
                                    waitCondition.await();
                                }
                            } finally {
                                waitLock.unlock();
                            }
                        } catch (InterruptedException e) {
                            // ignore
                        }
                    }

                }
            } finally {
                infoMessage("Worker thread stopped");
            }
        }

        private void shutdown() {
            Thread.currentThread().interrupt();
        }

        private void infoMessage(String messageFormat, Object... args) {
            if (LOGGER.isInfoEnabled()) {
                String formattedMessage = String.format(messageFormat, args);
                LOGGER.info(formattedMessage);
            }
        }
    }
}
