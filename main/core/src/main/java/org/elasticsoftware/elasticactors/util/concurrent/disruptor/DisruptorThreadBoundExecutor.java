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

package org.elasticsoftware.elasticactors.util.concurrent.disruptor;

import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.dsl.Disruptor;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import jakarta.annotation.PostConstruct;
import org.elasticsoftware.elasticactors.util.concurrent.ThreadBoundEvent;
import org.elasticsoftware.elasticactors.util.concurrent.ThreadBoundEventProcessor;
import org.elasticsoftware.elasticactors.util.concurrent.ThreadBoundRunnableEventProcessor;
import org.elasticsoftware.elasticactors.util.concurrent.metrics.CountingTimedThreadBoundExecutor;
import org.elasticsoftware.elasticactors.util.concurrent.metrics.ThreadBoundExecutorMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Joost van de Wijgerd
 */
public final class DisruptorThreadBoundExecutor extends CountingTimedThreadBoundExecutor {
    private static final Logger logger = LoggerFactory.getLogger(DisruptorThreadBoundExecutor.class);
    private final AtomicBoolean shuttingDown = new AtomicBoolean(false);
    private final ThreadFactory threadFactory;
    private final Disruptor<ThreadBoundEventWrapper>[] disruptors;
    private final ThreadBoundEventTranslator translator = new ThreadBoundEventTranslator();

    public DisruptorThreadBoundExecutor(
        ThreadFactory threadFactory,
        int workers,
        @Nullable ThreadBoundExecutorMonitor monitor)
    {
        this(
            new ThreadBoundRunnableEventProcessor(),
            1024,
            threadFactory,
            workers,
            monitor
        );
    }

    public DisruptorThreadBoundExecutor(
        ThreadBoundEventProcessor eventProcessor,
        int bufferSize,
        ThreadFactory threadFactory,
        int workers,
        @Nullable ThreadBoundExecutorMonitor monitor)
    {
        super(eventProcessor, monitor);
        this.threadFactory = threadFactory;
        this.disruptors = new Disruptor[workers];

        ThreadBoundEventWrapperFactory eventFactory = new ThreadBoundEventWrapperFactory();

        for (int i = 0; i < workers; i++) {
            Disruptor<ThreadBoundEventWrapper> disruptor = new Disruptor<>(eventFactory,bufferSize,threadFactory);
            disruptor.handleEventsWith(new ThreadBoundEventHandler(i, this::processBatch, bufferSize));
            disruptors[i] = disruptor;
            disruptor.start();
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
        return "disruptor";
    }

    @Override
    protected long getCapacityForThread(int thread, long currentCount) {
        return disruptors[thread].getBufferSize() - currentCount;
    }

    @Override
    protected long getQueuedEventsForThread(int thread, long currentCount) {
        // Just use the counting provided without modification
        return currentCount;
    }

    @Override
    protected boolean isShuttingDown() {
        return shuttingDown.get();
    }

    @Override
    protected void timedExecute(final int thread, @Nonnull final ThreadBoundEvent event) {
        // this method will wait when the buffer is overflowing ( using Lock.parkNanos(1) )
        disruptors[thread].getRingBuffer().publishEvent(translator, event);
    }

    @Override
    public void shutdown() {
        logger.info("Shutting down the {}[{}]", getClass().getSimpleName(), threadFactory);
        if (shuttingDown.compareAndSet(false, true)) {
            for (Disruptor<ThreadBoundEventWrapper> disruptor : disruptors) {
                // @todo: we may want to have a timeout here
                disruptor.shutdown();
            }
        }
        logger.info("{}[{}] shut down completed", getClass().getSimpleName(), threadFactory);
    }

    @Override
    public int getThreadCount() {
        return disruptors.length;
    }

    private static final class ThreadBoundEventTranslator implements EventTranslatorOneArg<ThreadBoundEventWrapper,ThreadBoundEvent> {

        @Override
        public void translateTo(ThreadBoundEventWrapper event, long sequence, ThreadBoundEvent eventToWrap) {
            event.setWrappedEvent(eventToWrap);
        }
    }
}
