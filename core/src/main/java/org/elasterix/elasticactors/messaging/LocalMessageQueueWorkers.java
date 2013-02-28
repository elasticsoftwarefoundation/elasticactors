/*
 * Copyright 2013 Joost van de Wijgerd
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

package org.elasterix.elasticactors.messaging;

import org.apache.log4j.Logger;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author Joost van de Wijgerd
 */
public final class LocalMessageQueueWorkers {
    private static final Logger LOGGER = Logger.getLogger(LocalMessageQueueWorkers.class);

    private final ExecutorService executor;
    private final int numberOfWorkers;
    private final RunnableWorker[] workers;
    private final AtomicInteger workerIndex = new AtomicInteger();

    private volatile boolean stop;

    public LocalMessageQueueWorkers(ExecutorService executor, int numberOfWorkers) {
        this.executor = executor;
        this.numberOfWorkers = Math.max(1, numberOfWorkers);
        workers = new RunnableWorker[this.numberOfWorkers];
    }

    @PostConstruct
    public void init() {
        stop = false;
        for (int i = 0; i < numberOfWorkers; i++) {
            workers[i] = new RunnableWorker();
            executor.submit(workers[i]);
        }
    }

    @PreDestroy
    public void destroy() {
        // @todo: this needs to be synchronized on the workers actually stopping
        stop = true;
        for (RunnableWorker worker : workers) {
            worker.shutdown();
        }
    }

    private RunnableWorker nextWorker() {
        return workers[Math.abs(workerIndex.getAndIncrement() % workers.length)];
    }

    public LocalMessageQueue create(String name,MessageHandler messageHandler) {
        // pick the next worker (round-robin)
        RunnableWorker worker = nextWorker();
        LocalMessageQueue messageQueue = new LocalMessageQueue(name,worker,messageHandler);
        // add the queue to the worker
        worker.add(messageQueue);
        return messageQueue;
    }

    private final class RunnableWorker implements Runnable, MessageQueueEventListener {
        private final ReentrantLock waitLock = new ReentrantLock();
        private final Condition waitCondition = waitLock.newCondition();
        private final ConcurrentMap<String,LocalMessageQueue> messageQueues;
        private final AtomicInteger pendingSignals = new AtomicInteger(0);

        public RunnableWorker() {
            messageQueues = new ConcurrentHashMap<String,LocalMessageQueue>();
        }

        public void add(LocalMessageQueue queue) {
            messageQueues.put(queue.getName(),queue);
        }

        @Override
        public void signal() {
            // register the signal to avoid race condition
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
            // @todo: we need to remove the queue, but it could be executing at this very moment
            // for now just remove it, this needs to become smarter
            messageQueues.remove(queue.getName());
        }

        @Override
        public void run() {
            try {
                while (!stop) {
                    // reset pending signals as we are starting the loop
                    pendingSignals.set(0);
                    boolean moreWaiting = false;
                    // make a fair loop
                    for (LocalMessageQueue messageQueue : messageQueues.values()) {
                        InternalMessage message = messageQueue.poll();
                        if (message != null) {
                            try {
                                messageQueue.getMessageHandler().handleMessage(message);
                            } catch (Exception e) {
                                LOGGER.error("Exception while executing work!", e);
                            } finally {
                                messageQueue.ack(message);
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
