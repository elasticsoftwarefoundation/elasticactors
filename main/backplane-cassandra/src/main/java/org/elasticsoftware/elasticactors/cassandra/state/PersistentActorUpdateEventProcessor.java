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

package org.elasticsoftware.elasticactors.cassandra.state;

import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ColumnFamilyUpdater;
import me.prettyprint.hector.api.beans.Composite;
import org.elasticsoftware.elasticactors.util.concurrent.ThreadBoundEventProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author Joost van de Wijgerd
 */
public final class PersistentActorUpdateEventProcessor implements ThreadBoundEventProcessor<PersistentActorUpdateEvent> {
    private static final Logger logger = LoggerFactory.getLogger(PersistentActorUpdateEventProcessor.class);
    private final ColumnFamilyTemplate<Composite,String> columnFamilyTemplate;

    public PersistentActorUpdateEventProcessor(ColumnFamilyTemplate<Composite, String> columnFamilyTemplate) {
        this.columnFamilyTemplate = columnFamilyTemplate;
    }

    @Override
    public void process(List<PersistentActorUpdateEvent> events) {
        Exception executionException = null;
        final long startTime = logger.isTraceEnabled() ? System.nanoTime() : 0L;
        try {
            ColumnFamilyUpdater<Composite, String> updater = columnFamilyTemplate.createUpdater();
            for (PersistentActorUpdateEvent event : events) {
                if(event.getPersistentActorBytes() != null) {
                    updater.addKey(event.getRowKey());
                    updater.setByteArray(event.getPersistentActorId(), event.getPersistentActorBytes());
                } else {
                    // it's a delete
                    updater.addKey(event.getRowKey());
                    updater.deleteColumn(event.getPersistentActorId());
                }
            }
            columnFamilyTemplate.update(updater);
        } catch(Exception e) {
            executionException = e;
        } finally {
            for (PersistentActorUpdateEvent event : events) {
                if(event.getEventListener() != null) {
                    if (executionException == null) {
                        event.getEventListener().onDone(event.getMessage());
                    } else {
                        event.getEventListener().onError(event.getMessage(), executionException);
                    }
                }
            }
            // add some trace info
            if(logger.isTraceEnabled()) {
                logger.trace(
                    "Updating {} Actor state entrie(s) took {} microsecs",
                    events.size(),
                    TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - startTime)
                );
            }
        }
    }

    @Override
    public void process(PersistentActorUpdateEvent... events) {
        process(Arrays.asList(events));
    }

    @Override
    public void process(PersistentActorUpdateEvent event) {
        process(Collections.singletonList(event));
    }
}
