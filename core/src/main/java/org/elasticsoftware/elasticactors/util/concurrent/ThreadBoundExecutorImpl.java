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

package org.elasticsoftware.elasticactors.util.concurrent;

import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Joost van de Wijgerd
 */
public final class ThreadBoundExecutorImpl implements ThreadBoundExecutor<String> {

    private static final Logger LOG = Logger.getLogger(ThreadBoundExecutorImpl.class);

    private final AtomicBoolean shuttingDown = new AtomicBoolean(false);
    private final List<BlockingQueue<Runnable>> queues = new ArrayList<BlockingQueue<Runnable>>();

    /**
     * Create an executor with numberOfThreads worker threads.
     *
     * @param numberOfThreads
     */
    public ThreadBoundExecutorImpl(ThreadFactory threadFactory, int numberOfThreads) {

        for (int i = 0; i < numberOfThreads; i++) {
            BlockingQueue<Runnable> queue = new LinkedBlockingQueue<Runnable>();
            Thread t = threadFactory.newThread(new Consumer(queue));
            queues.add(queue);
            t.start();
        }
    }

    /**
     * Schedule a runnable for execution.
     *
     * @param runnable The runnable to execute
     */
    public void execute(ThreadBoundRunnable<String> runnable) {
        if (shuttingDown.get()) {
            throw new RejectedExecutionException("The system is shutting down.");
        }
        int bucket = getBucket(runnable.getKey());
        BlockingQueue<Runnable> queue = queues.get(bucket);
        queue.add(runnable);
    }


    private int getBucket(Object key) {
        return Math.abs(key.hashCode()) % queues.size();
    }

    public void shutdown() {
        LOG.info("shutting down the ThreadBoundExecutor");
        if (shuttingDown.compareAndSet(false, true)) {
            final CountDownLatch shuttingDownLatch = new CountDownLatch(queues.size());
            for (BlockingQueue<Runnable> queue : queues) {
                queue.add(new ShutdownTask(shuttingDownLatch));
            }
            try {
                if (!shuttingDownLatch.await(30, TimeUnit.SECONDS)) {
                    LOG.error("timeout while waiting for ThreadBoundExecutor queues to empty");
                }
            } catch (InterruptedException ignore) {
                //we are shutting down anyway
                LOG.warn("ThreadBoundExecutor shutdown interrupted.");
            }
        }
        LOG.info("ThreadBoundExecutor shut down completed");
    }


    private static final class Consumer implements Runnable {
        private final BlockingQueue<Runnable> queue;

        public Consumer(BlockingQueue<Runnable> queue) {
            this.queue = queue;
        }

        @Override
        public void run() {
            try {
                boolean running = true;
                Runnable r = null;
                while (running) {
                    try {
                        r = queue.take();
                        r.run();
                    } catch (InterruptedException e) {
                        LOG.warn(String.format("Consumer on queue %s interrupted.", Thread.currentThread().getName()));
                        //ignore
                    } catch (Throwable exception) {
                        LOG.error(String.format("exception on queue %s while executing runnable: %s", Thread.currentThread().getName(), r), exception);
                    } finally {
                        if (r != null && r.getClass().equals(ShutdownTask.class)) {
                            running = false;
                        }
                        // reset r so that we get a correct null value if the queue.take() is interrupted
                        r = null;
                    }
                }
            } catch(Throwable unexpectedThrowable) {
                // we observed some cases where trying to log the inner exception threw an error
                // don't use the logger here as that seems to be causing the problem in the first place
                System.err.println("Caught and unexpected Throwable while logging");
                System.err.println("This problem happens when jar files change at runtime, JVM might be UNSTABLE");
                unexpectedThrowable.printStackTrace(System.err);
            }
        }
    }

    private static final class ShutdownTask implements Runnable {

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
    }

}
