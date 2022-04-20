/*
 *   Copyright 2013 - 2022 The Original Authors
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

package org.elasticsoftware.elasticactors.util.concurrent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @author Joost van de Wijgerd
 */
public final class ThreadBoundRunnableEventProcessor implements ThreadBoundEventProcessor<ThreadBoundRunnable> {
    private static final Logger logger = LoggerFactory.getLogger(
        BlockingQueueThreadBoundExecutor.class);

    @Override
    public void process(List<ThreadBoundRunnable> events) {
        for (ThreadBoundRunnable event : events) {
            process(event);
        }
    }

    @Override
    public void process(ThreadBoundRunnable event) {
        try {
            event.run();
        } catch (Throwable exception) {
            logger.error("Exception on queue {} while executing runnable: {}", Thread.currentThread().getName(), event, exception);
        }
    }
}