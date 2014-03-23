package org.elasticsoftware.elasticactors.cluster.scheduler;

import java.util.UUID;

/**
 * @author Joost van de Wijgerd
 */
public final class ScheduledMessageKey {
    private final UUID id;
    private final long fireTime;

    public ScheduledMessageKey(UUID id, long fireTime) {
        this.id = id;
        this.fireTime = fireTime;
    }

    public UUID getId() {
        return id;
    }

    public long getFireTime() {
        return fireTime;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ScheduledMessageKey that = (ScheduledMessageKey) o;

        if (fireTime != that.fireTime) return false;
        if (!id.equals(that.id)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = id.hashCode();
        result = 31 * result + (int) (fireTime ^ (fireTime >>> 32));
        return result;
    }
}
