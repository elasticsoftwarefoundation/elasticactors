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