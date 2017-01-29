/*
 * Copyright 2013 - 2017 The Original Authors
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
    public long getFireTime() {
        return 0;
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
