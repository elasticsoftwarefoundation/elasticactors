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

package org.elasticsoftware.elasticactors.cassandra4.cluster.scheduler;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import org.elasticsoftware.elasticactors.ShardKey;
import org.elasticsoftware.elasticactors.cluster.scheduler.ScheduledMessage;
import org.elasticsoftware.elasticactors.cluster.scheduler.ScheduledMessageKey;
import org.elasticsoftware.elasticactors.cluster.scheduler.ScheduledMessageRepository;
import org.elasticsoftware.elasticactors.serialization.internal.ScheduledMessageDeserializer;
import org.elasticsoftware.elasticactors.serialization.internal.ScheduledMessageSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.elasticsoftware.elasticactors.cassandra4.util.ExecutionUtils.executeWithRetry;

import static java.util.Objects.requireNonNull;


/**
 * @author Joost van de Wijgerd
 */
public final class CassandraScheduledMessageRepository implements ScheduledMessageRepository {
    private static final Logger logger = LoggerFactory.getLogger(CassandraScheduledMessageRepository.class);
    private final String clusterName;
    private final CqlSession cassandraSession;
    private final PreparedStatement insertStatement;
    private final PreparedStatement deleteStatement;
    private final PreparedStatement selectStatement;
    private final ScheduledMessageDeserializer scheduledMessageDeserializer;

    public CassandraScheduledMessageRepository(String clusterName, CqlSession cassandraSession, ScheduledMessageDeserializer scheduledMessageDeserializer) {
        this.clusterName = clusterName;
        this.cassandraSession = cassandraSession;
        this.scheduledMessageDeserializer = scheduledMessageDeserializer;
        this.insertStatement = cassandraSession.prepare("INSERT INTO \"ScheduledMessages\" (key, key2, column1, column2, value) VALUES (?, ?, ?, ?, ?)");
        this.deleteStatement = cassandraSession.prepare("DELETE FROM \"ScheduledMessages\" WHERE key = ? AND key2 = ? AND column1 = ? AND column2 = ?");
        this.selectStatement = cassandraSession.prepare("SELECT value from \"ScheduledMessages\" WHERE key = ? AND key2 = ?");
    }

    @Override
    public void create(ShardKey shardKey, ScheduledMessage scheduledMessage) {
        executeWithRetry(cassandraSession, insertStatement.bind(clusterName, shardKey.toString(), scheduledMessage.getFireTime(TimeUnit.MILLISECONDS), scheduledMessage.getId(), ByteBuffer.wrap(ScheduledMessageSerializer.get().serialize(scheduledMessage))), logger);
    }

    @Override
    public void delete(ShardKey shardKey, ScheduledMessageKey scheduledMessageKey) {
        executeWithRetry(cassandraSession, deleteStatement.bind(clusterName, shardKey.toString(), scheduledMessageKey.getFireTime(), scheduledMessageKey.getId()), logger);
    }

    @Override
    public List<ScheduledMessage> getAll(ShardKey shardKey) {
        ResultSet resultSet = executeWithRetry(cassandraSession, selectStatement.bind(clusterName, shardKey.toString()).setPageSize(Integer.MAX_VALUE), logger);
        List<ScheduledMessage> resultList = new LinkedList<>();
        for (Row resultRow : resultSet) {
            for (int i = 0; i < resultRow.getColumnDefinitions().size(); i++) {
                ByteBuffer resultBuffer = resultRow.getByteBuffer(i);
                try {
                    resultList.add(scheduledMessageDeserializer.deserialize(requireNonNull(resultBuffer)));
                } catch (NullPointerException | IOException e) {
                    logger.error("IOException while deserializing ScheduledMessage", e);
                }
            }
        }
        return resultList;
    }
}
