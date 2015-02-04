/*
 * Copyright 2013 - 2014 The Original Authors
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

package org.elasticsoftware.elasticactors.util.concurrent;

import org.apache.log4j.Logger;

import java.util.List;

/**
 * @author Joost van de Wijgerd
 */
public final class ThreadBoundRunnableEventProcessor implements ThreadBoundEventProcessor<ThreadBoundRunnable> {
    private static final Logger logger = Logger.getLogger(ThreadBoundExecutorImpl.class);

    @Override
    public void process(List<ThreadBoundRunnable> events) {
        for (ThreadBoundRunnable r : events) {
            try {
                r.run();
            } catch (Throwable exception) {
                logger.error(String.format("exception on queue %s while executing runnable: %s", Thread.currentThread().getName(), r), exception);
            }
        }
    }

    @Override
    public void process(ThreadBoundRunnable... events) {
        for (ThreadBoundRunnable event : events) {
            try {
                event.run();
            } catch (Throwable exception) {
                logger.error(String.format("exception on queue %s while executing runnable: %s", Thread.currentThread().getName(), event), exception);
            }
        }
    }
}
