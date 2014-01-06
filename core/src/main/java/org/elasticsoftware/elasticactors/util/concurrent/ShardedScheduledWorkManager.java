package org.elasticsoftware.elasticactors.util.concurrent;

import org.apache.log4j.Logger;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static java.lang.String.format;

/**
 * A WorkManager impl
 *
 * @param <K>   Key for a shard
 * @param <T>   The Scheduled Object that will be passed to {@link WorkExecutor#execute(Object)} after the delay has expired
 */
public final class ShardedScheduledWorkManager<K,T extends Delayed> {
    private static final Logger LOGGER = Logger.getLogger(ShardedScheduledWorkManager.class);
    public static final long MAX_AWAIT_MILLIS = 60000L;

    private final ExecutorService executor;
    private final WorkExecutorFactory<WorkExecutor<T>> workerFactory;
    private final int numberOfWorkers;

    private final ConcurrentMap<K,DelayQueue<T>> delayQueues;
    private final List<Future<?>> futures;
    private volatile boolean stop;

    private final ReentrantLock waitLock = new ReentrantLock();
    private final Condition waitCondition =  waitLock.newCondition();

    public ShardedScheduledWorkManager(ExecutorService executor, WorkExecutorFactory<WorkExecutor<T>> workerFactory, int numberOfWorkers){
        this.executor = executor;
        this.numberOfWorkers = Math.max(1,numberOfWorkers);
        this.workerFactory = workerFactory;
        futures = new ArrayList<>();
        delayQueues = new ConcurrentHashMap<>();
    }

    @PostConstruct
    public void init() {
        stop = false;
        for (int i = 0; i < numberOfWorkers; i++) {
            Future<?> future = executor.submit(new RunnableWorker(workerFactory.create()));
            futures.add(future);
        }
    }

    @PreDestroy
    public void destroy() {
        LOGGER.info("calling ShardedScheduledWorkManager.destroy()");
        stop = true;
        try {
            for (Future<?> f : futures) {
                try {
                    f.cancel(true);
                } catch (Exception e) {
                }// ignore
            }
            delayQueues.clear();
        } catch (Exception e) {
            LOGGER.error("Error on destroy", e);
        }
    }

    public void registerShard(K shard) {
        delayQueues.putIfAbsent(shard,new DelayQueue<T>());
    }

    public void unregisterShard(K shard) {
        delayQueues.remove(shard);
    }

    public void schedule(K shard, T... unitsOfWork) {
        final DelayQueue<T> delayQueue = this.delayQueues.get(shard);
        if(delayQueue == null) {
            throw new RejectedExecutionException("Shard: "+ shard.toString() + " is not registered, please call registerShard first");
        }
        // add all
        for (T unitOfWork : unitsOfWork) {
            delayQueue.add(unitOfWork);
        }
        // wake up a waiting thread
        try {
            final ReentrantLock waitLock = ShardedScheduledWorkManager.this.waitLock;
            waitLock.lockInterruptibly();
            try {
                waitCondition.signal();
            } finally {
                waitLock.unlock();
            }
        } catch(InterruptedException e) {
            // ignore
        }

    }

    public int getSize() {
        int totalSize = 0;
        for (DelayQueue<T> delayQueue : delayQueues.values()) {
            totalSize += delayQueue.size();
        }
        return totalSize;
    }

    private final class RunnableWorker implements Runnable {
        private final WorkExecutor<T> workExecutor;

        public RunnableWorker(WorkExecutor<T> workExecutor){
            this.workExecutor = workExecutor;
        }

        @Override
        public void run() {
            try {
                while (!stop) {
                    T work = null;
                    long waitTimeMillis = MAX_AWAIT_MILLIS;

                    for (DelayQueue<T> delayQueue : delayQueues.values()) {
                        work = delayQueue.poll();
                        if(work != null) {
                            try {
                                workExecutor.execute(work);
                            } catch(Exception e) {
                                LOGGER.error("Exception while executing work!", e);
                            }
                        } else {
                            // peek the head
                            Delayed d = delayQueue.peek();
                            if(d != null) {
                                waitTimeMillis = Math.min(d.getDelay(TimeUnit.MILLISECONDS),waitTimeMillis);
                            }
                        }
                    }
                    // all queues visited, now block until something happens
                    try {
                        final ReentrantLock waitLock = ShardedScheduledWorkManager.this.waitLock;
                        waitLock.lockInterruptibly();
                        try {
                            waitCondition.await(waitTimeMillis,TimeUnit.MILLISECONDS);
                        } finally {
                            waitLock.unlock();
                        }
                    } catch(InterruptedException e) {
                        // ignore
                    }

                }
            } finally {
                infoMessage("Worker thread stopped");
            }
        }

        private void infoMessage(String messageFormat, Object... args) {
            if (LOGGER.isInfoEnabled()) {
                String formattedMessage = format(messageFormat, args);
                LOGGER.info(formattedMessage);
            }
        }
    }
}
