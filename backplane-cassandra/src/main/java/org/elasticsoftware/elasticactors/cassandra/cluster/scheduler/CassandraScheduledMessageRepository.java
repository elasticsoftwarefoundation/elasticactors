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

package org.elasticsoftware.elasticactors.cassandra.cluster.scheduler;

import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.TimeUUIDSerializer;
import me.prettyprint.cassandra.service.template.ColumnFamilyResult;
import me.prettyprint.cassandra.service.template.ColumnFamilyRowMapper;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ColumnFamilyUpdater;
import me.prettyprint.hector.api.beans.Composite;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsoftware.elasticactors.ShardKey;
import org.elasticsoftware.elasticactors.cluster.scheduler.ScheduledMessage;
import org.elasticsoftware.elasticactors.cluster.scheduler.ScheduledMessageKey;
import org.elasticsoftware.elasticactors.cluster.scheduler.ScheduledMessageRepository;
import org.elasticsoftware.elasticactors.serialization.internal.ScheduledMessageDeserializer;
import org.elasticsoftware.elasticactors.serialization.internal.ScheduledMessageSerializer;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * @author Joost van de Wijgerd
 */
public final class CassandraScheduledMessageRepository implements ScheduledMessageRepository {
    private static final Logger logger = LogManager.getLogger(CassandraScheduledMessageRepository.class);
    private final String clusterName;
    private final ColumnFamilyTemplate<Composite,Composite> columnFamilyTemplate;
    private final ListResultMapper resultMapper = new ListResultMapper();
    private final ScheduledMessageDeserializer scheduledMessageDeserializer;

    public CassandraScheduledMessageRepository(String clusterName, ColumnFamilyTemplate<Composite, Composite> columnFamilyTemplate, ScheduledMessageDeserializer scheduledMessageDeserializer) {
        this.clusterName = clusterName;
        this.columnFamilyTemplate = columnFamilyTemplate;
        this.scheduledMessageDeserializer = scheduledMessageDeserializer;
    }

    @Override
    public void create(ShardKey shardKey, ScheduledMessage scheduledMessage) {
        final ColumnFamilyUpdater<Composite,Composite> updater = columnFamilyTemplate.createUpdater(createKey(shardKey));
        final Composite columnName = createColumnName(scheduledMessage);
        updater.setByteArray(columnName, ScheduledMessageSerializer.get().serialize(scheduledMessage));
        columnFamilyTemplate.update(updater);
    }

    @Override
    public void delete(ShardKey shardKey, ScheduledMessageKey scheduledMessageKey) {
        columnFamilyTemplate.deleteColumn(createKey(shardKey),createColumnName(scheduledMessageKey));
    }

    @Override
    public List<ScheduledMessage> getAll(ShardKey shardKey) {
        return columnFamilyTemplate.queryColumns(createKey(shardKey),resultMapper);
    }

    private Composite createKey(ShardKey shardKey) {
        Composite composite = new Composite();
        composite.add(clusterName);
        composite.add(shardKey.toString());
        return composite;
    }

    private Composite createColumnName(ScheduledMessage scheduledMessage) {
        final Composite columnName = new Composite();
        columnName.addComponent(scheduledMessage.getFireTime(TimeUnit.MILLISECONDS), LongSerializer.get());
        UUID id = scheduledMessage.getId();
        final com.eaio.uuid.UUID timeUuid = new com.eaio.uuid.UUID(id.getMostSignificantBits(),id.getLeastSignificantBits());
        columnName.addComponent(timeUuid, TimeUUIDSerializer.get());
        return columnName;
    }

    private Composite createColumnName(ScheduledMessageKey scheduledMessageKey) {
        final Composite columnName = new Composite();
        columnName.addComponent(scheduledMessageKey.getFireTime(), LongSerializer.get());
        UUID id = scheduledMessageKey.getId();
        final com.eaio.uuid.UUID timeUuid = new com.eaio.uuid.UUID(id.getMostSignificantBits(),id.getLeastSignificantBits());
        columnName.addComponent(timeUuid, TimeUUIDSerializer.get());
        return columnName;
    }

    private final class ListResultMapper implements ColumnFamilyRowMapper<Composite,Composite,List<ScheduledMessage>> {

        @Override
        public List<ScheduledMessage> mapRow(final ColumnFamilyResult<Composite, Composite> results) {
            List<ScheduledMessage> resultList = new LinkedList<>();

            if(results.hasResults()) {
                Collection<Composite> scheduledMessages = results.getColumnNames();
                for (Composite columnName : scheduledMessages) {
                    try {
                        resultList.add(scheduledMessageDeserializer.deserialize(results.getByteArray(columnName)));
                    } catch(IOException e)  {
                        logger.error(e);
                    }
                }
            }
            return resultList;
        }
    }
}
