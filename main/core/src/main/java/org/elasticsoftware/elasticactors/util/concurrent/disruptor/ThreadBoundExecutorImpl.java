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

package org.elasticsoftware.elasticactors.util.concurrent.disruptor;

import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import org.elasticsoftware.elasticactors.util.concurrent.ThreadBoundEvent;
import org.elasticsoftware.elasticactors.util.concurrent.ThreadBoundEventProcessor;
import org.elasticsoftware.elasticactors.util.concurrent.ThreadBoundExecutor;
import org.elasticsoftware.elasticactors.util.concurrent.ThreadBoundRunnable;
import org.elasticsoftware.elasticactors.util.concurrent.ThreadBoundRunnableEventProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsoftware.elasticactors.util.concurrent.TraceThreadBoundRunnable.wrap;

/**
 * @author Joost van de Wijgerd
 */
public final class ThreadBoundExecutorImpl implements ThreadBoundExecutor {
    private static final Logger logger = LoggerFactory.getLogger(ThreadBoundExecutorImpl.class);
    private final AtomicBoolean shuttingDown = new AtomicBoolean(false);
    private final ThreadFactory threadFactory;
    private final Disruptor<ThreadBoundEventWrapper>[] disruptors;
    private final ThreadBoundEventTranslator translator = new ThreadBoundEventTranslator();

    public ThreadBoundExecutorImpl(ThreadFactory threadFactory, int workers) {
        this(new ThreadBoundRunnableEventProcessor(), 1024, threadFactory, workers);
    }

    public ThreadBoundExecutorImpl(ThreadBoundEventProcessor eventProcessor, int bufferSize, ThreadFactory threadFactory, int workers) {
        this.threadFactory = threadFactory;
        this.disruptors = new Disruptor[workers];

        logger.info("Initializing (Disruptor)ThreadBoundExecutor[{}]",threadFactory);
        ThreadBoundEventWrapperFactory eventFactory = new ThreadBoundEventWrapperFactory();

        for (int i = 0; i < workers; i++) {
            Disruptor<ThreadBoundEventWrapper> disruptor = new Disruptor<>(eventFactory,bufferSize,threadFactory);
            disruptor.handleEventsWith(new ThreadBoundEventHandler(eventProcessor, bufferSize));
            disruptors[i] = disruptor;
            disruptor.start();
        }
    }

    @Override
    public void execute(ThreadBoundEvent event) {
        if (shuttingDown.get()) {
            throw new RejectedExecutionException("The system is shutting down.");
        }
        if (event instanceof ThreadBoundRunnable) {
            event = wrap((ThreadBoundRunnable<?>) event);
        }
        final RingBuffer<ThreadBoundEventWrapper> ringBuffer = disruptors[getBucket(event.getKey())].getRingBuffer();
        // this method will wait when the buffer is overflowing ( using Lock.parkNanos(1) )
        ringBuffer.publishEvent(translator, event);
    }

    @Override
    public void shutdown() {
        logger.info("Shutting down the (Disruptor)ThreadBoundExecutor[{}]",threadFactory);
        if (shuttingDown.compareAndSet(false, true)) {
            for (Disruptor<ThreadBoundEventWrapper> disruptor : disruptors) {
                // @todo: we may want to have a timeout here
                disruptor.shutdown();
            }
        }
        logger.info("(Disruptor)ThreadBoundExecutor[{}] shut down completed",threadFactory);
    }

    @Override
    public int getThreadCount() {
        return disruptors.length;
    }

    private int getBucket(Object key) {
        return Math.abs(key.hashCode()) % disruptors.length;
    }

    private static final class ThreadBoundEventTranslator implements EventTranslatorOneArg<ThreadBoundEventWrapper,ThreadBoundEvent> {

        @Override
        public void translateTo(ThreadBoundEventWrapper event, long sequence, ThreadBoundEvent eventToWrap) {
            event.setWrappedEvent(eventToWrap);
        }
    }
}
