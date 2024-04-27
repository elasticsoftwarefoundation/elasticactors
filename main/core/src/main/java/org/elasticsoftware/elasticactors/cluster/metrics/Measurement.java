/*
 * Copyright 2013 - 2024 The Original Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */

package org.elasticsoftware.elasticactors.cluster.metrics;

import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * @author Joost van de Wijgerd
 */
public final class Measurement {
    private final long creationTime;
    private long executionStart;
    private long executionEnd;
    private long serializationEnd;
    private long ackEnd;

    public Measurement(long creationTime) {
        this.creationTime = creationTime;
    }

    public void setExecutionStart(long executionStart) {
        this.executionStart = executionStart;
    }

    public void setExecutionEnd(long executionEnd) {
        this.executionEnd = executionEnd;
    }

    public void setSerializationEnd(long serializationEnd) {
        this.serializationEnd = serializationEnd;
    }

    public void setAckEnd(long ackEnd) {
        this.ackEnd = ackEnd;
    }

    public boolean isSerialized() {
        return serializationEnd != 0L;
    }

    public long getQueueDuration(TimeUnit unit) {
        return unit.convert(executionStart - creationTime, NANOSECONDS);
    }

    public long getExecutionDuration(TimeUnit unit) {
        return unit.convert(executionEnd - executionStart, NANOSECONDS);
    }

    public long getSerializationDuration(TimeUnit unit) {
        return (serializationEnd != 0)
            ? unit.convert(serializationEnd - executionEnd, NANOSECONDS)
            : 0L;
    }

    public long getAckDuration(TimeUnit unit) {
        return (ackEnd != 0) ? unit.convert(ackEnd - executionEnd, NANOSECONDS) : 0L;
    }

    public long getTotalDuration(TimeUnit unit) {
        long endTime = (ackEnd != 0)
            ? ackEnd
            : (serializationEnd != 0)
                ? serializationEnd
                : (executionEnd != 0)
                    ? executionEnd
                    : creationTime;
        return unit.convert(endTime - creationTime, NANOSECONDS);
    }

    public String summary(TimeUnit timeUnit) {
        String timeUnitStr = timeUnit.name().toLowerCase();
        return String.format(
            "Measurement summary: took %d %s in queue, "
                + "%d %s to execute, "
                + "%d %s to serialize and "
                + "%d %s to ack "
                + "(state update: %b)",
            getQueueDuration(timeUnit),
            timeUnitStr,
            getExecutionDuration(timeUnit),
            timeUnitStr,
            getSerializationDuration(timeUnit),
            timeUnitStr,
            getAckDuration(timeUnit),
            timeUnitStr,
            isSerialized()
        );
    }
}
