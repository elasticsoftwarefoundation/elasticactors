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

package org.elasticsoftware.elasticactors.cluster.scheduler;

import io.micrometer.core.instrument.binder.jvm.ExecutorServiceMetrics;
import org.elasticsoftware.elasticactors.ActorContainer;
import org.elasticsoftware.elasticactors.ActorContainerRef;
import org.elasticsoftware.elasticactors.ActorContextHolder;
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.ActorShard;
import org.elasticsoftware.elasticactors.ShardKey;
import org.elasticsoftware.elasticactors.cluster.metrics.MicrometerConfiguration;
import org.elasticsoftware.elasticactors.messaging.AbstractTracedMessage;
import org.elasticsoftware.elasticactors.messaging.UUIDTools;
import org.elasticsoftware.elasticactors.scheduler.ScheduledMessageRef;
import org.elasticsoftware.elasticactors.tracing.MessagingContextManager.MessagingScope;
import org.elasticsoftware.elasticactors.util.concurrent.DaemonThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static org.elasticsoftware.elasticactors.tracing.CreationContext.forScheduling;
import static org.elasticsoftware.elasticactors.tracing.MessagingContextManager.getManager;
/**
 * Simple in-memory scheduler that is backed by a {@link java.util.concurrent.ScheduledExecutorService}
 *
 * @author Joost van de Wijgerd
 */
public final class SimpleScheduler implements SchedulerService,ScheduledMessageRefFactory {
    private static final Logger logger = LoggerFactory.getLogger(SimpleScheduler.class);
    private ScheduledExecutorService scheduledExecutorService;
    private final ConcurrentMap<String, ScheduledFuture<?>> scheduledFutures =
            new ConcurrentHashMap<>();
    private final MicrometerConfiguration micrometerConfiguration;

    @PostConstruct
    public void init() {
        logger.info("Initializing Simple Actor Cluster Scheduler");
        ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor(new DaemonThreadFactory("SCHEDULER"));
        if (micrometerConfiguration != null) {
            scheduledExecutorService = ExecutorServiceMetrics.monitor(
                micrometerConfiguration.getRegistry(),
                scheduler,
                micrometerConfiguration.getComponentName(),
                micrometerConfiguration.getMetricPrefix(),
                micrometerConfiguration.getTags()
            );
        } else {
            scheduledExecutorService = scheduler;
        }
    }

    @PreDestroy
    public void destroy() {
        logger.info("Destroying Simple Actor Cluster Scheduler");
        scheduledExecutorService.shutdownNow();
    }

    public SimpleScheduler(@Nullable MicrometerConfiguration micrometerConfiguration) {
        this.micrometerConfiguration = micrometerConfiguration;
    }

    @Override
    public void registerShard(ShardKey shardKey) {
        // do nothing as this implementation can only be used in a single (test) instance
    }

    @Override
    public void unregisterShard(ShardKey shardKey) {
        // do nothing as this implementation can only be used in a single (test) instance
    }

    @Override
    public ScheduledMessageRef scheduleOnce(
        Object message,
        ActorRef receiver,
        long delay,
        TimeUnit timeUnit)
    {
        ActorRef sender = ActorContextHolder.getSelf();
        if (sender instanceof ActorContainerRef) {
            ActorContainer actorContainer = ((ActorContainerRef) sender).getActorContainer();
            if (actorContainer instanceof ActorShard) {
                ActorShard actorShard = (ActorShard) actorContainer;
                if (actorShard.getOwningNode().isLocal()) {
                    String id = UUIDTools.createRandomUUIDString();
                    ScheduledFuture scheduledFuture =
                        scheduledExecutorService.schedule(
                            new TellActorTask(id, sender, receiver, message),
                            delay,
                            timeUnit
                        );
                    scheduledFutures.put(id, scheduledFuture);
                    return new SimpleScheduledMessageRef(id, scheduledFuture);
                }
            }
        }
        throw new IllegalStateException(
            "Cannot determine an appropriate ActorRef(self). Only use this method while inside an "
                + "ElasticActor Lifecycle or on(Message) method on a Persistent Actor!");
    }

    @Override
    public void cancel(ShardKey shardKey, ScheduledMessageKey messageKey) {
        // do nothing as the SimpleScheduledMessageRef will cancel the message directly on the scheduler
    }

    @Override
    public ScheduledMessageRef create(String refSpec) {
        // refSpec == id
        ScheduledFuture scheduledFuture = scheduledFutures.get(refSpec);
        return new SimpleScheduledMessageRef(refSpec,scheduledFuture);
    }

    private final class TellActorTask extends AbstractTracedMessage implements Runnable {
        private final String id;
        private final ActorRef sender;
        private final ActorRef receiver;
        private final Object message;

        private TellActorTask(String id, ActorRef sender, ActorRef receiver, Object message) {
            this.id = id;
            this.sender = sender;
            this.receiver = receiver;
            this.message = message;
        }

        @Override
        public void run() {
            try (MessagingScope ignored = getManager().enter(
                    getTraceContext(),
                    forScheduling(getCreationContext()))) {
                runInContext();
            }
        }

        private void runInContext() {
            try {
                receiver.tell(message, sender);
                scheduledFutures.remove(id);
            } catch (Exception e) {
                logger.error("Exception sending scheduled messsage", e);
            }
        }

        @Nullable
        @Override
        public ActorRef getSender() {
            return sender;
        }

        @Override
        public String getTypeAsString() {
            return message.getClass().getName();
        }

        @Nullable
        @Override
        public Class<?> getType() {
            return message.getClass();
        }
    }
}
