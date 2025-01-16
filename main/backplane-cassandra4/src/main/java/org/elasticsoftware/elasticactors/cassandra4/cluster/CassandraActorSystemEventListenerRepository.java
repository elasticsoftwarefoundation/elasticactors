/*
 * Copyright 2013 - 2025 The Original Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */

package org.elasticsoftware.elasticactors.cassandra4.cluster;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.ShardKey;
import org.elasticsoftware.elasticactors.cluster.ActorSystemEvent;
import org.elasticsoftware.elasticactors.cluster.ActorSystemEventListener;
import org.elasticsoftware.elasticactors.cluster.ActorSystemEventListenerRepository;
import org.elasticsoftware.elasticactors.serialization.internal.ActorSystemEventListenerDeserializer;
import org.elasticsoftware.elasticactors.serialization.internal.ActorSystemEventListenerSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

import static java.util.Objects.requireNonNull;
import static org.elasticsoftware.elasticactors.cassandra4.util.ExecutionUtils.executeWithRetry;

/**
 * @author Joost van de Wijgerd
 */
public final class CassandraActorSystemEventListenerRepository implements ActorSystemEventListenerRepository {
    private static final Logger logger = LoggerFactory.getLogger(CassandraActorSystemEventListenerRepository.class);
    public static final String INSERT_QUERY = "INSERT INTO \"ActorSystemEventListeners\" (key, key2, key3, column1, value) VALUES (?, ?, ?, ?, ?)";
    public static final String DELETE_QUERY = "DELETE FROM \"ActorSystemEventListeners\" WHERE key = ? AND key2 = ? AND key3 = ? AND column1 = ?";
    public static final String SELECT_QUERY = "SELECT value FROM \"ActorSystemEventListeners\" WHERE key = ? and key2 = ? and key3 = ?";
    private final String clusterName;
    private final CqlSession cassandraSession;
    private final PreparedStatement insertStatement;
    private final PreparedStatement deleteStatement;
    private final PreparedStatement selectStatement;

    public CassandraActorSystemEventListenerRepository(String clusterName, CqlSession cassandraSession) {
        this.clusterName = clusterName;
        this.cassandraSession = cassandraSession;
        this.insertStatement = cassandraSession.prepare(INSERT_QUERY);
        this.deleteStatement = cassandraSession.prepare(DELETE_QUERY);
        this.selectStatement = cassandraSession.prepare(SELECT_QUERY);
    }


    @Override
    public void create(ShardKey shardKey, ActorSystemEvent event, ActorSystemEventListener listener) {
        byte[] value = ActorSystemEventListenerSerializer.get().serialize(listener);
        executeWithRetry(cassandraSession, insertStatement.bind(clusterName, shardKey.toString(), event.name(), listener.getActorId(), ByteBuffer.wrap(value)), logger);
    }

    @Override
    public void delete(ShardKey shardKey, ActorSystemEvent event, ActorRef listenerId) {
        executeWithRetry(cassandraSession, deleteStatement.bind(clusterName, shardKey.toString(), event.name(), listenerId.getActorId()), logger);
    }

    @Override
    public List<ActorSystemEventListener> getAll(ShardKey shardKey, ActorSystemEvent event) {
        ResultSet resultSet = executeWithRetry(cassandraSession, selectStatement.bind(clusterName, shardKey.toString(), event.name()).setPageSize(Integer.MAX_VALUE), logger);
        List<ActorSystemEventListener> resultList = new LinkedList<>();
        for (Row resultRow : resultSet) {
            for (int i = 0; i < resultRow.getColumnDefinitions().size(); i++) {
                ByteBuffer resultBuffer = resultRow.getByteBuffer(i);
                try {
                    resultList.add(ActorSystemEventListenerDeserializer.get().deserialize(requireNonNull(resultBuffer)));
                } catch(NullPointerException | IOException e)  {
                    logger.error("IOException while deserializing ActorSystemEventListener",e);
                }
            }
        }
        return resultList;
    }
}
