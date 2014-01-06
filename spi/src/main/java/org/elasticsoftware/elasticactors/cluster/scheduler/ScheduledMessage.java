package org.elasticsoftware.elasticactors.cluster.scheduler;

import org.elasticsoftware.elasticactors.ActorRef;

import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

/**
 * @author Joost van de Wijgerd
 */
public interface ScheduledMessage extends Delayed {
    /**
     * The UUID (time based) for this message
     *
     * @return
     */
    UUID getId();

    /**
     * The receiver of the scheduled message
     *
     * @return
     */
    ActorRef getReceiver();

    /**
     * The message
     *
     * @return
     */
    ByteBuffer getMessageBytes();

    /**
     * The sender
     *
     * @return
     */
    ActorRef getSender();

    /**
     * The absolute time on which this message should be send
     *
     * @param timeUnit
     * @return
     */
    long getFireTime(TimeUnit timeUnit);

    Class getMessageClass();
}
