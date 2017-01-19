/*
 * Copyright 2013 - 2016 The Original Authors
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

package org.elasticsoftware.elasticactors.cluster.scheduler;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.ShardKey;
import org.elasticsoftware.elasticactors.scheduler.ScheduledMessageRef;
import org.elasticsoftware.elasticactors.util.concurrent.DaemonThreadFactory;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.UUID;
import java.util.concurrent.*;

/**
 * Simple in-memory scheduler that is backed by a {@link java.util.concurrent.ScheduledExecutorService}
 *
 * @author Joost van de Wijgerd
 */
public final class SimpleScheduler implements SchedulerService,ScheduledMessageRefFactory {
    private static final Logger logger = LogManager.getLogger(SimpleScheduler.class);
    private ScheduledExecutorService scheduledExecutorService;
    private final ConcurrentMap<String,ScheduledFuture> scheduledFutures = new ConcurrentHashMap<>();

    @PostConstruct
    public void init() {
        scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new DaemonThreadFactory("SIMPLE-SCHEDULER"));
    }

    @PreDestroy
    public void destroy() {
        scheduledExecutorService.shutdown();
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
    public ScheduledMessageRef scheduleOnce(ActorRef sender,Object message, ActorRef receiver, long delay, TimeUnit timeUnit) {
        String id = UUID.randomUUID().toString();
        ScheduledFuture scheduledFuture = scheduledExecutorService.schedule(new TellActorTask(id, sender,receiver,message),delay,timeUnit);
        scheduledFutures.put(id,scheduledFuture);
        return new SimpleScheduledMessageRef(id,scheduledFuture);
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

    private final class TellActorTask implements Runnable {
        private final String id;
        private final ActorRef sender;
        private final ActorRef reciever;
        private final Object message;

        private TellActorTask(String id, ActorRef sender, ActorRef reciever, Object message) {
            this.id = id;
            this.sender = sender;
            this.reciever = reciever;
            this.message = message;
        }

        @Override
        public void run() {
            try {
                reciever.tell(message,sender);
                scheduledFutures.remove(id);
            } catch (Exception e) {
                logger.error("Exception sending scheduled messsage",e);
            }
        }
    }
}
