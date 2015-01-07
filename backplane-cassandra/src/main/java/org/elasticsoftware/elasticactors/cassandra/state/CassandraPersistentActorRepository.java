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

package org.elasticsoftware.elasticactors.cassandra.state;

import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ColumnFamilyUpdater;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.exceptions.HPoolRecoverableException;
import me.prettyprint.hector.api.exceptions.HTimedOutException;
import org.apache.log4j.Logger;
import org.elasticsoftware.elasticactors.ShardKey;
import org.elasticsoftware.elasticactors.messaging.InternalMessage;
import org.elasticsoftware.elasticactors.messaging.MessageHandlerEventListener;
import org.elasticsoftware.elasticactors.serialization.Deserializer;
import org.elasticsoftware.elasticactors.serialization.Serializer;
import org.elasticsoftware.elasticactors.state.PersistentActor;
import org.elasticsoftware.elasticactors.state.PersistentActorRepository;
import org.elasticsoftware.elasticactors.util.concurrent.ThreadBoundExecutor;

import java.io.IOException;

import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;


/**
 * @author Joost van de Wijgerd
 */
public final class CassandraPersistentActorRepository implements PersistentActorRepository {
    private static final Logger logger = Logger.getLogger(CassandraPersistentActorRepository.class);
    private final String clusterName;
    private final ThreadBoundExecutor asyncUpdateExecutor;
    private final long readExecutionThresholdMillis;
    private ColumnFamilyTemplate<Composite,String> columnFamilyTemplate;
    private Deserializer<byte[],PersistentActor> deserializer;
    private Serializer<PersistentActor,byte[]> serializer;

    public CassandraPersistentActorRepository(String clusterName, ThreadBoundExecutor asyncUpdateExecutor) {
        this(clusterName,asyncUpdateExecutor,200);
    }

    public CassandraPersistentActorRepository(String clusterName, ThreadBoundExecutor asyncUpdateExecutor, long readExecutionThresholdMillis) {
        this.clusterName = clusterName;
        this.asyncUpdateExecutor = asyncUpdateExecutor;
        this.readExecutionThresholdMillis = readExecutionThresholdMillis;
    }

    @Override
    public boolean contains(final ShardKey shard,final String actorId) {
        return querySingleColumnWithRetry(shard,actorId) != null;
    }

    @Override
    public void update(final ShardKey shard,final PersistentActor persistentActor) throws IOException {
        ColumnFamilyUpdater<Composite,String> updater = columnFamilyTemplate.createUpdater(createKey(shard));
        updater.setByteArray(persistentActor.getSelf().getActorId(), serializer.serialize(persistentActor));
        columnFamilyTemplate.update(updater);
    }

    @Override
    public void updateAsync(ShardKey shard, PersistentActor persistentActor, InternalMessage message, MessageHandlerEventListener messageHandlerEventListener) throws IOException {
        // serialize the data on the calling thread (to avoid thread visibility issues)
        final byte[] serializedActorBytes = serializer.serialize(persistentActor);
        asyncUpdateExecutor.execute(new PersistentActorUpdateEvent(createKey(shard),shard,
                                                                   persistentActor.getSelf().getActorId(),
                                                                   serializedActorBytes, message,
                                                                   messageHandlerEventListener));
    }

    @Override
    public void delete(final ShardKey shard,final String actorId) {
        columnFamilyTemplate.deleteColumn(createKey(shard),actorId);
    }

    @Override
    public PersistentActor<ShardKey> get(final ShardKey shard,final String actorId) throws IOException {
        HColumn<String,byte[]> column = querySingleColumnWithRetry(shard, actorId);
        if(column != null) {
            return deserializer.deserialize(column.getValue());
        } else {
            return null;
        }
    }

    private HColumn<String,byte[]> querySingleColumnWithRetry(final ShardKey shard,final String actorId) {
        // try three times, and log a warning when we exceed the readExecutionThreshold
        final long startTime = currentTimeMillis();
        int attemptsRemaining = 3;
        try {
            while (true) {
                attemptsRemaining--;
                try {
                    return columnFamilyTemplate.querySingleColumn(createKey(shard), actorId, BytesArraySerializer.get());
                } catch (HTimedOutException | HPoolRecoverableException e) {
                    if (attemptsRemaining <= 0) {
                        throw e;
                    }
                }
            }
        } finally {
            final long endTime = currentTimeMillis();
            if((endTime - startTime) > readExecutionThresholdMillis) {
                logger.warn(format("Cassandra read operation took %d msecs (%d retries) for actorId [%s] on shard [%s]",(endTime - startTime),(2 - attemptsRemaining),actorId,shard.toString()));
            }
        }
    }

    private Composite createKey(ShardKey shardKey) {
        Composite composite = new Composite();
        composite.add(clusterName);
        composite.add(shardKey.toString());
        return composite;
    }

    public void setColumnFamilyTemplate(ColumnFamilyTemplate<Composite, String> columnFamilyTemplate) {
        this.columnFamilyTemplate = columnFamilyTemplate;
    }

    public void setDeserializer(Deserializer deserializer) {
        this.deserializer = deserializer;
    }

    public void setSerializer(Serializer serializer) {
        this.serializer = serializer;
    }

}
