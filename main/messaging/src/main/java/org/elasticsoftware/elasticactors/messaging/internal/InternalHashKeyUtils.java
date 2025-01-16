/*
 * Copyright 2013 - 2025 The Original Authors
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

package org.elasticsoftware.elasticactors.messaging.internal;

import jakarta.annotation.Nullable;

public final class InternalHashKeyUtils {

    private InternalHashKeyUtils() {
    }

    /**
     * Return the key of an object if it implements {@link MessageQueueBoundPayload}.
     */
    @Nullable
    public static String getMessageQueueAffinityKey(@Nullable Object object) {
        if (object instanceof MessageQueueBoundPayload) {
            String key = ((MessageQueueBoundPayload) object).getMessageQueueAffinityKey();
            if (key != null && !key.isEmpty()) {
                return key;
            }
        }
        return null;
    }

}
