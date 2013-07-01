/*
 * Copyright 2013 the original authors
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

import org.apache.log4j.Logger;
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.scheduler.Scheduler;
import org.elasticsoftware.elasticactors.util.concurrent.DaemonThreadFactory;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Simple in-memory scheduler that is backed by a {@link java.util.concurrent.ScheduledExecutorService}
 *
 * @author Joost van de Wijgerd
 */
public final class SimpleScheduler implements Scheduler {
    private static final Logger logger = Logger.getLogger(SimpleScheduler.class);
    private ScheduledExecutorService scheduledExecutorService;

    @PostConstruct
    public void init() {
        scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new DaemonThreadFactory("SIMPLE-SCHEDULER"));
    }

    @PreDestroy
    public void destroy() {
        scheduledExecutorService.shutdown();
    }

    @Override
    public void scheduleOnce(ActorRef sender,Object message, ActorRef receiver, long delay, TimeUnit timeUnit) {
        scheduledExecutorService.schedule(new TellActorTask(sender,receiver,message),delay,timeUnit);
    }

    private static final class TellActorTask implements Runnable {
        private final ActorRef sender;
        private final ActorRef reciever;
        private final Object message;

        private TellActorTask(ActorRef sender, ActorRef reciever, Object message) {
            this.sender = sender;
            this.reciever = reciever;
            this.message = message;
        }

        @Override
        public void run() {
            try {
                reciever.tell(message,sender);
            } catch (Exception e) {
                logger.error("Exception sending scheduled messsage",e);
            }
        }
    }
}
