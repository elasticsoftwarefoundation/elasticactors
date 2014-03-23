package org.elasticsoftware.elasticactors.scheduler;

/**
 * Reference to a message that has been scheduled to be send in the future via the {@link Scheduler}.
 * The reference can be stored an used to cancel the scheduled message
 *
 * @author Joost van de Wijgerd
 */
public interface ScheduledMessageRef {
    String REFSPEC_FORMAT = "message://%s/%s/shards/%d/%d/%s";

    void cancel() throws Exception;
}
