/*
 * Copyright 2013 - 2015 The Original Authors
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

package org.elasticsoftware.elasticactors.util.concurrent.disruptor;

import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsoftware.elasticactors.util.concurrent.ThreadBoundEvent;
import org.elasticsoftware.elasticactors.util.concurrent.ThreadBoundEventProcessor;
import org.elasticsoftware.elasticactors.util.concurrent.ThreadBoundExecutor;
import org.elasticsoftware.elasticactors.util.concurrent.ThreadBoundRunnableEventProcessor;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.lang.String.format;

/**
 * @author Joost van de Wijgerd
 */
public final class ThreadBoundExecutorImpl implements ThreadBoundExecutor {
    private static final Logger LOG = LogManager.getLogger(ThreadBoundExecutorImpl.class);
    private final AtomicBoolean shuttingDown = new AtomicBoolean(false);
    private final ThreadFactory threadFactory;
    private final List<Disruptor<ThreadBoundEventWrapper>> disruptors;
    private final ThreadBoundEventTranslator translator = new ThreadBoundEventTranslator();

    public ThreadBoundExecutorImpl(ThreadFactory threadFactory, int workers) {
        this(new ThreadBoundRunnableEventProcessor(), 1024, threadFactory, workers);
    }

    public ThreadBoundExecutorImpl(ThreadBoundEventProcessor eventProcessor, int bufferSize, ThreadFactory threadFactory, int workers) {
        this.threadFactory = threadFactory;
        this.disruptors = new ArrayList<>(workers);

        LOG.info(format("Initializing (Disruptor)ThreadBoundExecutor[%s]",threadFactory.toString()));
        ThreadBoundEventWrapperFactory eventFactory = new ThreadBoundEventWrapperFactory();

        Executor executor = Executors.newCachedThreadPool(threadFactory);
        for (int i = 0; i < workers; i++) {
            Disruptor<ThreadBoundEventWrapper> disruptor = new Disruptor<>(eventFactory,bufferSize,executor);
            disruptor.handleEventsWith(new ThreadBoundEventHandler(eventProcessor, bufferSize));
            this.disruptors.add(disruptor);
            disruptor.start();
        }
    }

    @Override
    public void execute(ThreadBoundEvent event) {
        if (shuttingDown.get()) {
            throw new RejectedExecutionException("The system is shutting down.");
        }
        final RingBuffer<ThreadBoundEventWrapper> ringBuffer = this.disruptors.get(getBucket(event.getKey())).getRingBuffer();
        // this method will wait when the buffer is overflowing ( using Lock.parkNanos(1) )
        ringBuffer.publishEvent(translator, event);
    }

    @Override
    public void shutdown() {
        LOG.info(format("shutting down the (Disruptor)ThreadBoundExecutor[%s]",threadFactory.toString()));
        if (shuttingDown.compareAndSet(false, true)) {
            for (Disruptor<ThreadBoundEventWrapper> disruptor : disruptors) {
                // @todo: we may want to have a timeout here
                disruptor.shutdown();
            }
        }
        LOG.info(format("(Disruptor)ThreadBoundExecutor[%s] shut down completed",threadFactory.toString()));
    }

    private int getBucket(Object key) {
        return Math.abs(key.hashCode()) % disruptors.size();
    }

    private static final class ThreadBoundEventTranslator implements EventTranslatorOneArg<ThreadBoundEventWrapper,ThreadBoundEvent> {

        @Override
        public void translateTo(ThreadBoundEventWrapper event, long sequence, ThreadBoundEvent eventToWrap) {
            event.setWrappedEvent(eventToWrap);
        }
    }
}
