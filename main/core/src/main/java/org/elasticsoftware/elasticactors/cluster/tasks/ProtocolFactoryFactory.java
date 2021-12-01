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

package org.elasticsoftware.elasticactors.cluster.tasks;

import org.elasticsoftware.elasticactors.cluster.tasks.app.ApplicationProtocolFactory;
import org.elasticsoftware.elasticactors.cluster.tasks.reactivestreams.ReactiveStreamsProtocolFactory;
import org.elasticsoftware.elasticactors.messaging.InternalMessage;

/**
 * @author Joost van de Wijgerd
 */
public final class ProtocolFactoryFactory {
    private final static ProtocolFactory applicationProtocolFactory = new ApplicationProtocolFactory();
    private final static ProtocolFactory reactiveStreamsProtocolFactory = new ReactiveStreamsProtocolFactory();

    public static ProtocolFactory getProtocolFactory(InternalMessage internalMessage) {
        if (internalMessage.isReactive()) {
            return reactiveStreamsProtocolFactory;
        } else {
            return applicationProtocolFactory;
        }
    }
}
