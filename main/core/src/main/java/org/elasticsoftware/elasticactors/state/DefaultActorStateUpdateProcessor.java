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

package org.elasticsoftware.elasticactors.state;

import io.micrometer.core.instrument.MeterRegistry;
import org.elasticsoftware.elasticactors.cluster.metrics.MeterTagCustomizer;
import org.elasticsoftware.elasticactors.util.concurrent.ThreadBoundEventProcessor;
import org.elasticsoftware.elasticactors.util.concurrent.ThreadBoundExecutor;
import org.elasticsoftware.elasticactors.util.concurrent.ThreadBoundExecutorBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.env.Environment;

import javax.annotation.Nullable;
import javax.annotation.PostConstruct;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

/**
 * @author Joost van de Wijgerd
 */
public final class DefaultActorStateUpdateProcessor implements ActorStateUpdateProcessor, ThreadBoundEventProcessor<ActorStateUpdateEvent> {
    private static final Logger logger = LoggerFactory.getLogger(DefaultActorStateUpdateProcessor.class);
    private final ThreadBoundExecutor<ActorStateUpdateEvent> executor;
    private final List<ActorStateUpdateListener> listeners = new ArrayList<>();
    private final Consumer<List<ActorStateUpdateEvent>> processingFunction;

    public DefaultActorStateUpdateProcessor(
        Collection<ActorStateUpdateListener> listeners,
        Environment env,
        @Nullable MeterRegistry meterRegistry,
        @Nullable MeterTagCustomizer tagCustomizer)
    {
        this.listeners.addAll(listeners);
        this.executor = ThreadBoundExecutorBuilder.buildBlockingQueueThreadBoundExecutor(
            env,
            this,
            "actorStateUpdateProcessor",
            "ACTORSTATE-UPDATE-WORKER",
            meterRegistry,
            tagCustomizer
        );
        // optimize in the case of one listener, copy otherwise to avoid possible concurrency issues on the serializedState ByteBuffer
        this.processingFunction = (listeners.size() == 1) ? this::processWithoutCopy : this::processWithCopy;
    }

    public DefaultActorStateUpdateProcessor(
        Collection<ActorStateUpdateListener> listeners,
        Environment env,
        int workerCount,
        int batchSize,
        @Nullable MeterRegistry meterRegistry,
        @Nullable MeterTagCustomizer tagCustomizer)
    {
        this.listeners.addAll(listeners);
        this.executor = ThreadBoundExecutorBuilder.buildBlockingQueueThreadBoundExecutor(
            env,
            this,
            workerCount,
            batchSize,
            "actorStateUpdateProcessor",
            "ACTORSTATE-UPDATE-WORKER",
            meterRegistry,
            tagCustomizer
        );
        // optimize in the case of one listener, copy otherwise to avoid possible concurrency issues on the serializedState ByteBuffer
        this.processingFunction = (listeners.size() == 1) ? this::processWithoutCopy : this::processWithCopy;
    }

    @PostConstruct
    public void init() {
        executor.init();
    }

    @Override
    public void process(@Nullable ActorLifecycleStep lifecycleStep, @Nullable Object message, PersistentActor persistentActor) {
        if(lifecycleStep == null && message == null) {
            throw new IllegalArgumentException("At least one of lifecycleStep or message needs to be not null");
        }
        this.executor.execute(new ActorStateUpdateEvent(persistentActor.getActorClass(),
                persistentActor.getSelf(),
                persistentActor.getSerializedState() != null ?
                        ByteBuffer.wrap(persistentActor.getSerializedState()).asReadOnlyBuffer() : null,
                persistentActor.getCurrentActorStateVersion(),
                lifecycleStep, message != null ? message.getClass() : null));
    }

    @Override
    public void process(List<ActorStateUpdateEvent> events) {
        processingFunction.accept(events);
    }

    @Override
    public void process(ActorStateUpdateEvent event) {
        process(Collections.singletonList(event));
    }

    private void processWithoutCopy(List<ActorStateUpdateEvent> events) {
        for (ActorStateUpdateListener listener : listeners) {
            try {
                listener.onUpdate(events);
            } catch(Exception e) {
                logger.error("Unexpected Exception while processing ActorStateUpdates on listener of type {}", listener.getClass().getSimpleName(), e);
            }
        }
    }

    private void processWithCopy(List<ActorStateUpdateEvent> events) {
        for (ActorStateUpdateListener listener : listeners) {
            try {
                List<ActorStateUpdateEvent> list = new ArrayList<>(events.size());
                for (ActorStateUpdateEvent event : events) {
                    list.add(event.copyOf());
                }
                listener.onUpdate(list);
            } catch(Exception e) {
                logger.error("Unexpected Exception while processing ActorStateUpdates on listener of type {}", listener.getClass().getSimpleName(), e);
            }
        }
    }
}
