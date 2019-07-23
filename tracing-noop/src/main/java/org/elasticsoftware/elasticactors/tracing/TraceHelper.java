/*
 *   Copyright 2013 - 2019 The Original Authors
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.elasticsoftware.elasticactors.tracing;

import org.elasticsoftware.elasticactors.messaging.InternalMessage;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public final class TraceHelper {

    public static void run(@Nonnull Runnable r, @Nullable InternalMessage m, @Nonnull String name) {
        r.run();
    }

    public static void onError(Throwable t) {
    }
}
