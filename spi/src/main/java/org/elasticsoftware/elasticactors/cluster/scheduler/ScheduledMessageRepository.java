package org.elasticsoftware.elasticactors.cluster.scheduler;

import java.util.List;

/**
 * @author Joost van de Wijgerd
 */
public interface ScheduledMessageRepository {
    void create(ScheduledMessage scheduledMessage);

    void delete(ScheduledMessage scheduledMessage);

    List<ScheduledMessage> getAll();
}
