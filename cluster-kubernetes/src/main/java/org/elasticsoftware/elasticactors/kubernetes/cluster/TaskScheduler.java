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

package org.elasticsoftware.elasticactors.kubernetes.cluster;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static java.lang.String.format;

public class TaskScheduler {

    private static final Logger logger = LogManager.getLogger(TaskScheduler.class);

    private final ScheduledExecutorService scheduledExecutorService;
    private final Integer timeoutSeconds;

    private final AtomicReference<ScheduledFuture> scheduledTask;

    public TaskScheduler(ScheduledExecutorService scheduledExecutorService, Integer timeoutSeconds) {
        this.scheduledTask = new AtomicReference<>();
        this.scheduledExecutorService = scheduledExecutorService;
        this.timeoutSeconds = timeoutSeconds;
    }

    private void replaceCurrentTask(ScheduledFuture<?> newTask) {
        ScheduledFuture<?> previous = scheduledTask.getAndSet(newTask);
        if (previous != null && previous.cancel(false)) {
            logger.info("Cancelling previously scheduled task");
        }
    }

    public void scheduleTask(Runnable runnable, Integer multiplier, String taskName) {
        ScheduledFuture<?> newTask = null;
        if (runnable != null) {
            int delay = multiplier * timeoutSeconds;
            logger.info(format("Scheduling task %s with a delay of %d seconds", taskName, delay));
            newTask = scheduledExecutorService.schedule(runnable, delay, TimeUnit.SECONDS);
        }
        replaceCurrentTask(newTask);
    }

    public void cancelScheduledTask() {
        replaceCurrentTask(null);
    }
}
