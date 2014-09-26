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
import org.elasticsoftware.elasticactors.ShardKey;
import org.elasticsoftware.elasticactors.serialization.Deserializer;
import org.elasticsoftware.elasticactors.serialization.Serializer;
import org.elasticsoftware.elasticactors.state.PersistentActor;
import org.elasticsoftware.elasticactors.state.PersistentActorRepository;

import java.io.IOException;


/**
 * @author Joost van de Wijgerd
 */
public final class CassandraPersistentActorRepository implements PersistentActorRepository {
    private final String clusterName;
    private ColumnFamilyTemplate<Composite,String> columnFamilyTemplate;
    private Deserializer<byte[],PersistentActor> deserializer;
    private Serializer<PersistentActor,byte[]> serializer;

    public CassandraPersistentActorRepository(String clusterName) {
        this.clusterName = clusterName;
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
        // try three times, and
        int attemptsRemaining = 3;
        while(true) {
            attemptsRemaining--;
            try {
                return columnFamilyTemplate.querySingleColumn(createKey(shard), actorId, BytesArraySerializer.get());
            } catch(HTimedOutException | HPoolRecoverableException e) {
                if(attemptsRemaining <= 0) {
                    throw e;
                }
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
