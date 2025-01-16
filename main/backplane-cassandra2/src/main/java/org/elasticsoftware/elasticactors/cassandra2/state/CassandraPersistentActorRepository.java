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

package org.elasticsoftware.elasticactors.cassandra2.state;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import org.elasticsoftware.elasticactors.ShardKey;
import org.elasticsoftware.elasticactors.cassandra.common.state.PersistentActorUpdateEvent;
import org.elasticsoftware.elasticactors.messaging.InternalMessage;
import org.elasticsoftware.elasticactors.messaging.MessageHandlerEventListener;
import org.elasticsoftware.elasticactors.serialization.Deserializer;
import org.elasticsoftware.elasticactors.serialization.Serializer;
import org.elasticsoftware.elasticactors.state.PersistentActor;
import org.elasticsoftware.elasticactors.state.PersistentActorRepository;
import org.elasticsoftware.elasticactors.util.concurrent.ThreadBoundExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import static org.elasticsoftware.elasticactors.cassandra2.util.ExecutionUtils.executeWithRetry;


/**
 * @author Joost van de Wijgerd
 */
public final class CassandraPersistentActorRepository implements PersistentActorRepository {
    private static final Logger logger = LoggerFactory.getLogger(CassandraPersistentActorRepository.class);
    private final String clusterName;
    private final ThreadBoundExecutor asyncUpdateExecutor;
    private final long readExecutionThresholdMillis;
    private final Session cassandraSession;
    private final PreparedStatement selectStatement;
    private final Deserializer<ByteBuffer,PersistentActor> deserializer;
    private final Serializer<PersistentActor,ByteBuffer> serializer;

    public CassandraPersistentActorRepository(Session cassandraSession, String clusterName, ThreadBoundExecutor asyncUpdateExecutor, Serializer serializer, Deserializer deserializer) {
        this(cassandraSession, clusterName,asyncUpdateExecutor,serializer, deserializer, 200);
    }

    public CassandraPersistentActorRepository(Session cassandraSession, String clusterName, ThreadBoundExecutor asyncUpdateExecutor, Serializer serializer, Deserializer deserializer, long readExecutionThresholdMillis) {
        this.cassandraSession = cassandraSession;
        this.selectStatement = cassandraSession.prepare("select value from \"PersistentActors\" where key = ? and key2 = ? AND column1 = ?");
        this.clusterName = clusterName;
        this.asyncUpdateExecutor = asyncUpdateExecutor;
        this.readExecutionThresholdMillis = readExecutionThresholdMillis;
        this.serializer = serializer;
        this.deserializer = deserializer;
    }

    @Override
    public boolean contains(final ShardKey shard,final String actorId) {
        return internalGet(shard, actorId) != null;
    }

    @Override
    public void update(final ShardKey shard,final PersistentActor persistentActor) throws IOException {
        // serialize the data on the calling thread (to avoid thread visibility issues)
        final ByteBuffer serializedActorBytes = serializer.serialize(persistentActor);
        asyncUpdateExecutor.execute(new PersistentActorUpdateEvent(createKey(shard), shard,
                                                                    persistentActor.getSelf().getActorId(),
                                                                    serializedActorBytes, null, null));
    }

    @Override
    public void updateAsync(ShardKey shard, PersistentActor persistentActor, InternalMessage message, MessageHandlerEventListener messageHandlerEventListener) throws IOException {
        // serialize the data on the calling thread (to avoid thread visibility issues)
        final ByteBuffer serializedActorBytes = serializer.serialize(persistentActor);
        asyncUpdateExecutor.execute(new PersistentActorUpdateEvent(createKey(shard),shard,
                                                                   persistentActor.getSelf().getActorId(),
                                                                   serializedActorBytes, message,
                                                                   messageHandlerEventListener));
    }

    @Override
    public void delete(final ShardKey shard,final String actorId) {
        asyncUpdateExecutor.execute(new PersistentActorUpdateEvent(createKey(shard), shard, actorId, null, null, null));
    }

    @Override
    public PersistentActor<ShardKey> get(final ShardKey shard,final String actorId) throws IOException {
        Row resultRow = internalGet(shard, actorId);
        if (resultRow == null || resultRow.getColumnDefinitions().size() == 0) {
            return null;
        } else {
            // should have only a single column here
            return this.deserializer.deserialize(resultRow.getBytes(0));
        }
    }

    private Row internalGet(final ShardKey shard,final String actorId) {
        // log a warning when we exceed the readExecutionThreshold
        final long startTime = System.nanoTime();
        try {
            ResultSet resultSet = executeWithRetry(cassandraSession, selectStatement.bind(clusterName, shard.toString(), actorId), logger);
            return resultSet.one();
        } finally {
            final long duration = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);
            if (duration > readExecutionThresholdMillis) {
                logger.warn(
                    "Cassandra read operation took {} msecs for actorId [{}] on shard [{}]",
                    duration,
                    actorId,
                    shard
                );
            }
        }
    }

    private String[] createKey(ShardKey shardKey) {
        String[] composite = new String[2];
        composite[0] = clusterName;
        composite[1] = shardKey.toString();
        return composite;
    }

}
