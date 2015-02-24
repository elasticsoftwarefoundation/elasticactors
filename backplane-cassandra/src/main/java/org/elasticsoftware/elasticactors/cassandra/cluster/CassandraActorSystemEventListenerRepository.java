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

package org.elasticsoftware.elasticactors.cassandra.cluster;

import me.prettyprint.cassandra.service.template.ColumnFamilyResult;
import me.prettyprint.cassandra.service.template.ColumnFamilyRowMapper;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ColumnFamilyUpdater;
import me.prettyprint.hector.api.beans.Composite;
import org.apache.log4j.Logger;
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.ShardKey;
import org.elasticsoftware.elasticactors.cluster.ActorSystemEvent;
import org.elasticsoftware.elasticactors.cluster.ActorSystemEventListener;
import org.elasticsoftware.elasticactors.cluster.ActorSystemEventListenerRepository;
import org.elasticsoftware.elasticactors.serialization.internal.ActorSystemEventListenerDeserializer;
import org.elasticsoftware.elasticactors.serialization.internal.ActorSystemEventListenerSerializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * @author Joost van de Wijgerd
 */
public final class CassandraActorSystemEventListenerRepository implements ActorSystemEventListenerRepository {
    private static final Logger logger = Logger.getLogger(CassandraActorSystemEventListenerRepository.class);
    private final String clusterName;
    private final ColumnFamilyTemplate<Composite,String> columnFamilyTemplate;
    private final ListResultMapper resultMapper = new ListResultMapper();

    public CassandraActorSystemEventListenerRepository(String clusterName, ColumnFamilyTemplate<Composite, String> columnFamilyTemplate) {
        this.clusterName = clusterName;
        this.columnFamilyTemplate = columnFamilyTemplate;
    }

    @Override
    public void create(ShardKey shardKey, ActorSystemEvent event, ActorSystemEventListener listener) {
        byte[] value = ActorSystemEventListenerSerializer.get().serialize(listener);
        ColumnFamilyUpdater<Composite,String> updater = columnFamilyTemplate.createUpdater(createKey(shardKey, event));
        updater.setByteArray(listener.getActorId(),value);
        columnFamilyTemplate.update(updater);
    }

    @Override
    public void delete(ShardKey shardKey, ActorSystemEvent event, ActorRef listenerId) {
        ColumnFamilyUpdater<Composite, String> updater = columnFamilyTemplate.createUpdater();
        updater.addKey(createKey(shardKey, event));
        updater.deleteColumn(listenerId.getActorId());
        columnFamilyTemplate.update(updater);
    }

    @Override
    public List<ActorSystemEventListener> getAll(ShardKey shardKey, ActorSystemEvent event) {
        return columnFamilyTemplate.queryColumns(createKey(shardKey, event),resultMapper);
    }

    private Composite createKey(ShardKey shardKey, ActorSystemEvent event) {
        Composite composite = new Composite();
        composite.add(clusterName);
        composite.add(shardKey.toString());
        composite.add(event.name());
        return composite;
    }

    private final class ListResultMapper implements ColumnFamilyRowMapper<Composite,String,List<ActorSystemEventListener>> {

        @Override
        public List<ActorSystemEventListener> mapRow(final ColumnFamilyResult<Composite, String> results) {
            List<ActorSystemEventListener> resultList = new ArrayList<>(1024);

            if(results.hasResults()) {
                Collection<String> actorIds = results.getColumnNames();
                for (String actorId : actorIds) {
                    try {
                        resultList.add(ActorSystemEventListenerDeserializer.get().deserialize(results.getByteArray(actorId)));
                    } catch(IOException e)  {
                        logger.error("IOException while deserializing ActorSystemEventListener",e);
                    }
                }
            }
            return resultList;
        }
    }
}
