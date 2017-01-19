/*
 * Copyright 2013 - 2016 The Original Authors
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

package org.elasticsoftware.elasticactors.cluster.tasks;

import javax.annotation.Nullable;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * @author Joost van de Wijgerd
 */
public final class Measurement {
    private final Long creationTime;
    private Long executionStart;
    private Long executionEnd;
    @Nullable
    private Long serializationEnd = null;
    @Nullable
    private Long ackEnd = null;


    public Measurement(Long creationTime) {
        this.creationTime = creationTime;
    }

    public void setExecutionStart(Long executionStart) {
        this.executionStart = executionStart;
    }

    public void setExecutionEnd(Long executionEnd) {
        this.executionEnd = executionEnd;
    }

    public void setSerializationEnd(@Nullable Long serializationEnd) {
        this.serializationEnd = serializationEnd;
    }

    public void setAckEnd(@Nullable Long ackEnd) {
        this.ackEnd = ackEnd;
    }

    public boolean isSerialized() {
        return serializationEnd != null;
    }

    public Long getQueueDuration(final TimeUnit unit) {
        return unit.convert(executionStart-creationTime, NANOSECONDS);
    }

    public Long getExecutionDuration(final TimeUnit unit) {
        return unit.convert(executionEnd-executionStart, NANOSECONDS);
    }

    public Long getSerializationDuration(final TimeUnit unit) {
        return (serializationEnd != null) ? unit.convert(serializationEnd-executionEnd, NANOSECONDS) : 0L;
    }

    public Long getAckDuration(final TimeUnit unit) {
        return (ackEnd != null) ? unit.convert(ackEnd-executionEnd, NANOSECONDS) : 0L;
    }

    public Long getTotalDuration(final TimeUnit unit) {
        Long endTime = (ackEnd != null) ? ackEnd : (serializationEnd != null) ? serializationEnd : creationTime;
        return unit.convert(endTime-creationTime,NANOSECONDS);
    }
}
