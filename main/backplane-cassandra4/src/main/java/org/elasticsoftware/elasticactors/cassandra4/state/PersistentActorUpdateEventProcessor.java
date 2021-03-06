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

package org.elasticsoftware.elasticactors.cassandra4.state;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import org.elasticsoftware.elasticactors.cassandra.common.state.PersistentActorUpdateEvent;
import org.elasticsoftware.elasticactors.util.concurrent.ThreadBoundEventProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.datastax.oss.driver.api.core.cql.BatchType.UNLOGGED;
import static org.elasticsoftware.elasticactors.cassandra4.util.ExecutionUtils.executeWithRetry;

import static java.lang.System.currentTimeMillis;

/**
 * @author Joost van de Wijgerd
 */
public final class PersistentActorUpdateEventProcessor implements ThreadBoundEventProcessor<PersistentActorUpdateEvent> {
    private static final Logger logger = LoggerFactory.getLogger(PersistentActorUpdateEventProcessor.class);
    public static final String INSERT_QUERY = "INSERT INTO \"PersistentActors\" (key, key2, column1, value) VALUES (?, ?, ?, ?)";
    public static final String DELETE_QUERY = "DELETE FROM \"PersistentActors\" where key = ? AND key2 = ? AND column1 = ?";
    private final CqlSession cassandraSession;
    private final PreparedStatement insertStatement;
    private final PreparedStatement deleteStatement;
    private final Map<Integer,PreparedStatement> batchStatements = new HashMap<>();
    private final boolean optimizedV1Batches;

    public PersistentActorUpdateEventProcessor(CqlSession cassandraSession, int maxBatchSize) {
        this(cassandraSession, maxBatchSize, true);
    }

    public PersistentActorUpdateEventProcessor(CqlSession cassandraSession, int maxBatchSize, boolean optimizedV1Batches) {
        this.cassandraSession = cassandraSession;
        this.insertStatement = cassandraSession.prepare(INSERT_QUERY);
        this.deleteStatement = cassandraSession.prepare(DELETE_QUERY);
        this.optimizedV1Batches = optimizedV1Batches;
    }

    @Override
    public void process(PersistentActorUpdateEvent... events) {
        process(Arrays.asList(events));
    }

    @Override
    public void process(List<PersistentActorUpdateEvent> events) {
        Exception executionException = null;
        final long startTime = currentTimeMillis();
        try {
            // optimized to use the prepared statement
            if(events.size() == 1) {
                PersistentActorUpdateEvent event = events.get(0);
                BoundStatement boundStatement;
                if(event.getPersistentActorBytes() != null) {
                    boundStatement = insertStatement.bind(event.getRowKey()[0], event.getRowKey()[1], event.getPersistentActorId(), event.getPersistentActorBytes());
                } else {
                    // it's a delete
                    boundStatement = deleteStatement.bind(event.getRowKey()[0], event.getRowKey()[1], event.getPersistentActorId());
                }
                // execute the statement
                executeWithRetry(cassandraSession, boundStatement, logger);
            } else {
                executeBatchV3AndUp(events);
            }
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
                final long endTime = currentTimeMillis();
                logger.trace("Updating {} Actor state entrie(s) took {} msecs", events.size(), endTime - startTime);
            }
        }
    }

    private void executeBatchV3AndUp(List<PersistentActorUpdateEvent> events) {
        BatchStatement batchStatement = BatchStatement.newInstance(UNLOGGED);
        for (PersistentActorUpdateEvent event : events) {
            if (event.getPersistentActorBytes() != null) {
                batchStatement.add(insertStatement.bind(event.getRowKey()[0], event.getRowKey()[1], event.getPersistentActorId(), event.getPersistentActorBytes()));
            } else {
                // it's a delete
                batchStatement.add(deleteStatement.bind(event.getRowKey()[0], event.getRowKey()[1], event.getPersistentActorId()));
            }
        }
        executeWithRetry(cassandraSession, batchStatement, logger);
    }



}
