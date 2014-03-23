package org.elasticsoftware.elasticactors.cluster.scheduler;

import org.elasticsoftware.elasticactors.scheduler.ScheduledMessageRef;

import javax.annotation.Nullable;
import java.util.concurrent.ScheduledFuture;

/**
 * @author Joost van de Wijgerd
 */
public final class SimpleScheduledMessageRef implements ScheduledMessageRef {
    private final String id;
    private final ScheduledFuture scheduledFuture;

    public SimpleScheduledMessageRef(String id,@Nullable ScheduledFuture scheduledFuture) {
        this.id = id;
        this.scheduledFuture = scheduledFuture;
    }

    @Override
    public void cancel() throws Exception {
        if(scheduledFuture != null) {
            scheduledFuture.cancel(false);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SimpleScheduledMessageRef that = (SimpleScheduledMessageRef) o;

        if (!id.equals(that.id)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    @Override
    public String toString() {
        return id;
    }
}
