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

package org.elasticsoftware.elasticactors.cluster.tasks.reactivestreams;

import org.elasticsoftware.elasticactors.SubscriberContext;
import org.elasticsoftware.elasticactors.SubscriberContextHolder;

/**
 * @author Joost van de Wijgerd
 */
public final class InternalSubscriberContext extends SubscriberContextHolder {
    private InternalSubscriberContext() {
        super();
    }

    protected static SubscriberContext setContext(SubscriberContext context) {
        final SubscriberContext currentContext = threadContext.get();
        threadContext.set(context);
        return currentContext;
    }

    protected static SubscriberContext getAndClearContext() {
        SubscriberContext state = threadContext.get();
        threadContext.set(null);
        return state;
    }
}
