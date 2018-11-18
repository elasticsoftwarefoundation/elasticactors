/*
 * Copyright 2013 - 2017 The Original Authors
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

package org.elasticsoftware.elasticactors.util.concurrent;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.lang.String.format;

/**
 * @author Joost van de Wijgerd
 */
public final class ThreadBoundExecutorImpl implements ThreadBoundExecutor {
    private static final Logger LOG = LogManager.getLogger(ThreadBoundExecutorImpl.class);
    private final ThreadFactory threadFactory;
    private final AtomicBoolean shuttingDown = new AtomicBoolean(false);
    private final List<BlockingQueue<ThreadBoundEvent>> queues = new ArrayList<>();

    /**
     * Create an executor with numberOfThreads worker threads.
     *
     * @param numberOfThreads
     */
    public ThreadBoundExecutorImpl(ThreadFactory threadFactory, int numberOfThreads) {
        this(new ThreadBoundRunnableEventProcessor(),1,threadFactory,numberOfThreads);
    }

    public ThreadBoundExecutorImpl(ThreadBoundEventProcessor eventProcessor, int maxBatchSize, ThreadFactory threadFactory, int numberOfThreads) {
        this.threadFactory = threadFactory;
        LOG.info(format("Initializing (LinkedBlockingQueue)ThreadBoundExecutor[%s]",threadFactory.toString()));
        for (int i = 0; i < numberOfThreads; i++) {
            BlockingQueue<ThreadBoundEvent> queue = new LinkedBlockingQueue<>();
            Thread t = threadFactory.newThread(new Consumer(queue,eventProcessor,maxBatchSize));
            queues.add(queue);
            t.start();
        }
    }

    /**
     * Schedule a runnable for execution.
     *
     * @param event The runnable to execute
     */
    public void execute(ThreadBoundEvent event) {
        if (shuttingDown.get()) {
            throw new RejectedExecutionException("The system is shutting down.");
        }
        int bucket = getBucket(event.getKey());
        BlockingQueue<ThreadBoundEvent> queue = queues.get(bucket);
        queue.add(event);
    }

    @Override
    public int getThreadCount() {
        return queues.size();
    }

    private int getBucket(Object key) {
        return queues.size() == 1 ? 1 : Math.abs(key.hashCode()) % queues.size();
    }

    public void shutdown() {
        LOG.info(format("shutting down the (LinkedBlockingQueue)ThreadBoundExecutor[%s]",threadFactory.toString()));
        if (shuttingDown.compareAndSet(false, true)) {
            final CountDownLatch shuttingDownLatch = new CountDownLatch(queues.size());
            for (BlockingQueue<ThreadBoundEvent> queue : queues) {
                queue.add(new ShutdownTask(shuttingDownLatch));
            }
            try {
                if (!shuttingDownLatch.await(30, TimeUnit.SECONDS)) {
                    LOG.error(format("timeout while waiting for (LinkedBlockingQueue)ThreadBoundExecutor[%s] queues to empty",threadFactory.toString()));
                }
            } catch (InterruptedException ignore) {
                //we are shutting down anyway
                LOG.warn(format("(LinkedBlockingQueue)ThreadBoundExecutor[%s] shutdown interrupted.",threadFactory.toString()));
            }
        }
        LOG.info(format("(LinkedBlockingQueue)ThreadBoundExecutor[%s] shut down completed",threadFactory.toString()));
    }


    private static final class Consumer implements Runnable {
        private final BlockingQueue<ThreadBoundEvent> queue;
        private final int maxBatchSize;
        private final ArrayList<ThreadBoundEvent> batch;
        private final ThreadBoundEventProcessor<ThreadBoundEvent> eventProcessor;

        public Consumer(BlockingQueue<ThreadBoundEvent> queue) {
            this(queue,new ThreadBoundRunnableEventProcessor(),1);
        }

        public Consumer(BlockingQueue<ThreadBoundEvent> queue, ThreadBoundEventProcessor eventProcessor, int maxBatchSize) {
            this.queue = queue;
            this.eventProcessor = eventProcessor;
            // store this -1 as we will always use take to get the first element of the batch
            this.maxBatchSize = maxBatchSize - 1;
            this.batch = new ArrayList<>(maxBatchSize);
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
                        batch.add(event);
                        if(maxBatchSize > 0) {
                            queue.drainTo(batch, maxBatchSize);
                        }
                        // check for the stop condition (and remove it)
                        // treat batches of 1 (the most common case) specially
                        if(batch.size() > 1) {
                            ListIterator<ThreadBoundEvent> itr = batch.listIterator();
                            while (itr.hasNext()) {
                                ThreadBoundEvent next = itr.next();
                                if (next.getClass().equals(ShutdownTask.class)) {
                                    running = false;
                                    ((ShutdownTask)next).latch.countDown();
                                    itr.remove();
                                }
                            }
                            eventProcessor.process(batch);
                        } else {
                            // just the one event, no need to iterate
                            if(event.getClass().equals(ShutdownTask.class)) {
                                running = false;
                                ((ShutdownTask)event).latch.countDown();
                            } else {
                                eventProcessor.process(batch);
                            }
                        }
                    } catch (InterruptedException e) {
                        LOG.warn(String.format("Consumer on queue %s interrupted.", Thread.currentThread().getName()));
                        //ignore
                    } catch (Throwable exception) {
                        LOG.error(String.format("exception on queue %s while executing events", Thread.currentThread().getName()), exception);
                    } finally {
                        // reset the batch
                        batch.clear();
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
            return "ShutdownTask";
        }

        @Override
        public Object getKey() {
            return null;
        }
    }

}
