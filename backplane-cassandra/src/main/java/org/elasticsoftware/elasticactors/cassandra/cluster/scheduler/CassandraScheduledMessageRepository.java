package org.elasticsoftware.elasticactors.cassandra.cluster.scheduler;

import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.TimeUUIDSerializer;
import me.prettyprint.cassandra.service.template.ColumnFamilyResult;
import me.prettyprint.cassandra.service.template.ColumnFamilyRowMapper;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ColumnFamilyUpdater;
import me.prettyprint.hector.api.beans.Composite;
import org.apache.log4j.Logger;
import org.elasticsoftware.elasticactors.ShardKey;
import org.elasticsoftware.elasticactors.cluster.scheduler.ScheduledMessage;
import org.elasticsoftware.elasticactors.cluster.scheduler.ScheduledMessageRepository;
import org.elasticsoftware.elasticactors.serialization.Deserializer;
import org.elasticsoftware.elasticactors.serialization.Serializer;
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
    private static final Logger logger = Logger.getLogger(CassandraScheduledMessageRepository.class);
    private final ColumnFamilyTemplate<String,Composite> columnFamilyTemplate;
    private final ListResultMapper resultMapper = new ListResultMapper();

    public CassandraScheduledMessageRepository(ColumnFamilyTemplate<String, Composite> columnFamilyTemplate) {
        this.columnFamilyTemplate = columnFamilyTemplate;
    }

    @Override
    public void create(ShardKey shardKey, ScheduledMessage scheduledMessage) {
        final ColumnFamilyUpdater<String,Composite> updater = columnFamilyTemplate.createUpdater(shardKey.toString());
        final Composite columnName = createColumnName(scheduledMessage);
        updater.setByteArray(columnName, ScheduledMessageSerializer.get().serialize(scheduledMessage));
        columnFamilyTemplate.update(updater);
    }

    @Override
    public void delete(ShardKey shardKey, ScheduledMessage scheduledMessage) {
        columnFamilyTemplate.deleteColumn(shardKey.toString(),createColumnName(scheduledMessage));
    }

    @Override
    public List<ScheduledMessage> getAll(ShardKey shardKey) {
        return columnFamilyTemplate.queryColumns(shardKey.toString(),resultMapper);
    }

    private Composite createColumnName(ScheduledMessage scheduledMessage) {
        final Composite columnName = new Composite();
        columnName.addComponent(scheduledMessage.getFireTime(TimeUnit.MILLISECONDS), LongSerializer.get());
        UUID id = scheduledMessage.getId();
        final com.eaio.uuid.UUID timeUuid = new com.eaio.uuid.UUID(id.getMostSignificantBits(),id.getLeastSignificantBits());
        columnName.addComponent(timeUuid, TimeUUIDSerializer.get());
        return columnName;
    }

    private final class ListResultMapper implements ColumnFamilyRowMapper<String,Composite,List<ScheduledMessage>> {

        @Override
        public List<ScheduledMessage> mapRow(final ColumnFamilyResult<String, Composite> results) {
            List<ScheduledMessage> resultList = new LinkedList<>();

            if(results.hasResults()) {
                Collection<Composite> scheduledMessages = results.getColumnNames();
                for (Composite columnName : scheduledMessages) {
                    try {
                        resultList.add(ScheduledMessageDeserializer.get().deserialize(results.getByteArray(columnName)));
                    } catch(IOException e)  {
                        logger.error(e);
                    }
                }
            }
            return resultList;
        }
    }
}
