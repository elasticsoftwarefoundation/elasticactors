/*
 * Copyright 2013 - 2024 The Original Authors
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

package org.elasticsoftware.elasticactors.util.concurrent;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import jakarta.annotation.PostConstruct;
import org.elasticsoftware.elasticactors.util.concurrent.metrics.DelegatingTimedThreadBoundExecutor;
import org.elasticsoftware.elasticactors.util.concurrent.metrics.ThreadBoundExecutorMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.ListIterator;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Joost van de Wijgerd
 */
public final class BlockingQueueThreadBoundExecutor
    extends DelegatingTimedThreadBoundExecutor {

    private static final Logger logger = LoggerFactory.getLogger(BlockingQueueThreadBoundExecutor.class);
    private final ThreadFactory threadFactory;
    private final AtomicBoolean shuttingDown = new AtomicBoolean(false);
    private final BlockingQueue<ThreadBoundEvent>[] queues;

    /**
     * Create an executor with numberOfThreads worker threads.
     *
     * @param threadFactory the thread factory to be used in this executor
     * @param numberOfThreads the number of worker threads
     */
    public BlockingQueueThreadBoundExecutor(
        ThreadFactory threadFactory,
        int numberOfThreads,
        @Nullable ThreadBoundExecutorMonitor monitor)
    {
        this(
            new ThreadBoundRunnableEventProcessor(),
            1,
            threadFactory,
            numberOfThreads,
            monitor
        );
    }

    public BlockingQueueThreadBoundExecutor(
        ThreadBoundEventProcessor eventProcessor,
        int maxBatchSize,
        ThreadFactory threadFactory,
        int numberOfThreads,
        @Nullable ThreadBoundExecutorMonitor monitor)
    {
        super(eventProcessor, monitor);
        this.threadFactory = threadFactory;
        this.queues = new BlockingQueue[numberOfThreads];
        for (int i = 0; i < numberOfThreads; i++) {
            BlockingQueue<ThreadBoundEvent> queue = new LinkedBlockingQueue<>();
            Thread t = threadFactory.newThread(new Consumer(i, queue, maxBatchSize));
            queues[i] = queue;
            t.start();
        }
    }

    @PostConstruct
    @Override
    public void init() {
        logger.info("Initializing {} [{}]", getClass().getSimpleName(), threadFactory);
        super.init();
    }

    @Override
    protected Logger getLogger() {
        return logger;
    }

    @Nonnull
    @Override
    protected String getExecutorDataStructureName() {
        return "queue";
    }

    @Override
    protected long getCapacityForThread(int thread) {
        return queues[thread].remainingCapacity();
    }

    @Override
    protected long getQueuedEventsForThread(int thread) {
        return queues[thread].size();
    }

    @Override
    protected boolean isShuttingDown() {
        return shuttingDown.get();
    }

    /**
     * Schedule a runnable for execution.
     *
     * @param event The runnable to execute
     */
    @Override
    protected void timedExecute(final int thread, @Nonnull final ThreadBoundEvent event) {
        queues[thread].add(event);
    }

    @Override
    public int getThreadCount() {
        return queues.length;
    }

    @Override
    public void shutdown() {
        logger.info("Shutting down the {}[{}]", getClass().getSimpleName(), threadFactory);
        if (shuttingDown.compareAndSet(false, true)) {
            final CountDownLatch shuttingDownLatch = new CountDownLatch(queues.length);
            for (BlockingQueue<ThreadBoundEvent> queue : queues) {
                queue.add(new ShutdownTask(shuttingDownLatch));
            }
            try {
                if (!shuttingDownLatch.await(30, TimeUnit.SECONDS)) {
                    logger.error(
                        "Timeout while waiting for {}[{}] queues to empty",
                        getClass().getSimpleName(),
                        threadFactory
                    );
                }
            } catch (InterruptedException ignore) {
                //we are shutting down anyway
                logger.warn(
                    "{}[{}] shutdown interrupted.",
                    getClass().getSimpleName(),
                    threadFactory
                );
            }
        }
        logger.info("{}[{}] shut down completed", getClass().getSimpleName(), threadFactory);
    }

    private final class Consumer implements Runnable {
        private final int thread;
        private final BlockingQueue<ThreadBoundEvent> queue;
        private final int maxBatchSize;
        private final ArrayList<ThreadBoundEvent> batch;

        public Consumer(int thread, BlockingQueue<ThreadBoundEvent> queue, int maxBatchSize) {
            this.thread = thread;
            this.queue = queue;
            // store this -1 as we will always use take to get the first element of the batch
            this.maxBatchSize = maxBatchSize - 1;
            this.batch = maxBatchSize > 1 ? new ArrayList<>(maxBatchSize) : null;
        }

        @Override
        public void run() {
            try {
                boolean running = true;
                while (running) {
                    try {
                        // block on event availability
                        ThreadBoundEvent event = queue.take();
                        // add to the batch, and see if we can add more
                        if (batch != null) {
                            batch.add(event);
                            queue.drainTo(batch, maxBatchSize);
                        }
                        // check for the stop condition (and remove it)
                        // treat batches of 1 (the most common case) specially
                        if (batch != null && batch.size() > 1) {
                            ListIterator<ThreadBoundEvent> itr = batch.listIterator();
                            while (itr.hasNext()) {
                                ThreadBoundEvent next = itr.next();
                                if (next instanceof ShutdownTask) {
                                    running = false;
                                    ((ShutdownTask)next).latch.countDown();
                                    itr.remove();
                                }
                            }
                            if (!batch.isEmpty()) {
                                processBatch(thread, batch);
                            }
                        } else {
                            // just the one event, no need to iterate
                            if (event instanceof ShutdownTask) {
                                running = false;
                                ((ShutdownTask)event).latch.countDown();
                            } else {
                                // This batch has only one item, but some processors will wrap
                                // single events into a list and then iterate over it.
                                // Let's just use the batch to avoid creating garbage.
                                if (batch != null) {
                                    processBatch(thread, batch);
                                } else {
                                    processEvent(thread, event);
                                }
                            }
                        }
                    } catch (InterruptedException e) {
                        logger.warn("Consumer on queue {} interrupted.", Thread.currentThread().getName());
                        //ignore
                    } catch (Throwable exception) {
                        logger.error("Exception on queue {} while executing events", Thread.currentThread().getName(), exception);
                    } finally {
                        // reset the batch
                        if (batch != null) {
                            batch.clear();
                        }
                    }
                }
            } catch(Throwable unexpectedThrowable) {
                // we observed some cases where trying to log the inner exception threw an error
                // don't use the logger here as that seems to be causing the problem in the first place
                System.err.println("Caught an unexpected Throwable while logging");
                System.err.println("This problem happens when jar files change at runtime, JVM might be UNSTABLE");
                unexpectedThrowable.printStackTrace(System.err);
            }
        }
    }

    private static final class ShutdownTask implements ThreadBoundRunnable<Object> {

        private final CountDownLatch latch;

        public ShutdownTask(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public void run() {
            latch.countDown();
        }

        @Override
        public String toString() {
            return ShutdownTask.class.getSimpleName();
        }

        @Override
        public Object getKey() {
            return null;
        }
    }

}
