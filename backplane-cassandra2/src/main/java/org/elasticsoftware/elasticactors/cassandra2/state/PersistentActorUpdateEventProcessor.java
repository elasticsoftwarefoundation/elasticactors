/*
 * Copyright 2013 - 2015 The Original Authors
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

package org.elasticsoftware.elasticactors.cassandra2.state;

import com.datastax.driver.core.*;
import org.apache.log4j.Logger;
import org.elasticsoftware.elasticactors.util.concurrent.ThreadBoundEventProcessor;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import static com.datastax.driver.core.BatchStatement.Type.UNLOGGED;
import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;

/**
 * @author Joost van de Wijgerd
 */
public final class PersistentActorUpdateEventProcessor implements ThreadBoundEventProcessor<PersistentActorUpdateEvent> {
    private static final Logger logger = Logger.getLogger(PersistentActorUpdateEventProcessor.class);
    public static final String INSERT_QUERY = "INSERT INTO PersistentActors (key, key2, column1, value) VALUES (?, ?, ?, ?)";
    public static final String DELETE_QUERY = "DELETE ? FROM PersistentActors where key = ? and key2 = ?";
    private final Session cassandraSession;
    private final PreparedStatement insertStatement;
    private final PreparedStatement deleteStatement;

    public PersistentActorUpdateEventProcessor(Session cassandraSession) {
        this.cassandraSession = cassandraSession;
        this.insertStatement = cassandraSession.prepare(INSERT_QUERY);
        this.deleteStatement = cassandraSession.prepare(DELETE_QUERY);
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
                cassandraSession.execute(boundStatement);
            } else {
                // check the protocol to see if BatchStatements are supported
                ProtocolVersion protocolVersion = cassandraSession.getCluster().getConfiguration().getProtocolOptions().getProtocolVersionEnum();
                if(ProtocolVersion.V1.equals(protocolVersion)) {
                    executeBatchV1(events);
                } else {
                    executeBatchV2AndUp(events);
                }
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
                logger.trace(format("Updating %d Actor state entrie(s) took %d msecs",events.size(),endTime-startTime));
            }
        }
    }

    private void executeBatchV1(List<PersistentActorUpdateEvent> events) {
        List<Object> arguments = new LinkedList<>();
        StringBuilder batchBuilder = new StringBuilder("BEGIN UNLOGGED BATCH ");
        for (PersistentActorUpdateEvent event : events) {
            batchBuilder.append("   ");
            if(event.getPersistentActorBytes() != null) {
                // insert query
                batchBuilder.append(INSERT_QUERY);
                // add the 4 arguments in order
                arguments.add(event.getRowKey()[0]);
                arguments.add(event.getRowKey()[1]);
                arguments.add(event.getPersistentActorId());
                arguments.add(event.getPersistentActorBytes());
            } else {
                // delete query
                batchBuilder.append(DELETE_QUERY);
                // add the 3 arguments in order
                arguments.add(event.getPersistentActorId());
                arguments.add(event.getRowKey()[0]);
                arguments.add(event.getRowKey()[1]);
            }
            batchBuilder.append("; ");
        }
        batchBuilder.append("APPLY BATCH");
        PreparedStatement batchStatement = cassandraSession.prepare(batchBuilder.toString());
        cassandraSession.execute(batchStatement.bind(arguments));
    }

    private void executeBatchV2AndUp(List<PersistentActorUpdateEvent> events) {
        BatchStatement batchStatement = new BatchStatement(UNLOGGED);
        for (PersistentActorUpdateEvent event : events) {
            if (event.getPersistentActorBytes() != null) {
                batchStatement.add(insertStatement.bind(event.getRowKey()[0], event.getRowKey()[1], event.getPersistentActorId(), event.getPersistentActorBytes()));
            } else {
                // it's a delete
                batchStatement.add(deleteStatement.bind(event.getRowKey()[0], event.getRowKey()[1], event.getPersistentActorId()));
            }
        }
        cassandraSession.execute(batchStatement);
    }

}
