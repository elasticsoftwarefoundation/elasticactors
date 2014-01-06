package org.elasticsoftware.elasticactors.cassandra.cluster.scheduler;

import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.hector.api.beans.Composite;
import org.elasticsoftware.elasticactors.cluster.scheduler.ScheduledMessage;
import org.elasticsoftware.elasticactors.cluster.scheduler.ScheduledMessageRepository;
import org.elasticsoftware.elasticactors.serialization.Deserializer;
import org.elasticsoftware.elasticactors.serialization.Serializer;
import org.elasticsoftware.elasticactors.state.PersistentActor;

import java.util.List;

/**
 * @author Joost van de Wijgerd
 */
public class CassandraScheduledMessageRepository implements ScheduledMessageRepository {
    private ColumnFamilyTemplate<String,Composite> columnFamilyTemplate;
    private Deserializer<byte[],ScheduledMessage> deserializer;
    private Serializer<ScheduledMessage,byte[]> serializer;

    @Override
    public void create(ScheduledMessage scheduledMessage) {
        //columnFamilyTemplate.
    }

    @Override
    public void delete(ScheduledMessage scheduledMessage) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public List<ScheduledMessage> getAll() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }
}
