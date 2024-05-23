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
        if (this == o) {
            return true;
        }
        if (!(o instanceof ScheduledMessageKey)) {
            return false;
        }

        ScheduledMessageKey that = (ScheduledMessageKey) o;

        if (fireTime != that.fireTime) {
            return false;
        }
        return id.equals(that.id);
    }

    @Override
    public int hashCode() {
        int result = id.hashCode();
        result = 31 * result + (int) (fireTime ^ (fireTime >>> 32));
        return result;
    }
}
