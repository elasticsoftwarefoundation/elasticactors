package org.elasticsoftware.elasticactors.cluster.scheduler;

import org.elasticsoftware.elasticactors.scheduler.ScheduledMessageRef;

/**
 * @author Joost van de Wijgerd
 */
public interface ScheduledMessageRefFactory {
    ScheduledMessageRef create(String refSpec);
}
