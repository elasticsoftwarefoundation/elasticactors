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

import com.lmax.disruptor.EventHandler;
import org.elasticsoftware.elasticactors.util.concurrent.ThreadBoundEvent;
import org.elasticsoftware.elasticactors.util.concurrent.ThreadBoundEventProcessor;
import org.elasticsoftware.elasticactors.util.concurrent.metrics.ThreadBoundEventUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Joost van de Wijgerd
 */
public final class ThreadBoundEventHandler implements EventHandler<ThreadBoundEventWrapper> {
    private final ThreadBoundEventProcessor delegate;
    private final List<ThreadBoundEvent> batch;

    public ThreadBoundEventHandler(ThreadBoundEventProcessor delegate, int maxBatchSize) {
        this.delegate = delegate;
        this.batch = new ArrayList<>(maxBatchSize);
    }

    @Override
    public void onEvent(ThreadBoundEventWrapper event, long sequence, boolean endOfBatch) throws Exception {
        batch.add(event.getWrappedEvent());
        if(endOfBatch) {
            try {
                ThreadBoundEventUtils.processBatch(delegate, batch);
            } finally {
                batch.clear();
            }
        }
    }
}
