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

package org.elasticsoftware.elasticactors.client.cluster;

import org.elasticsoftware.elasticactors.ActorSystemConfiguration;
import org.elasticsoftware.elasticactors.RemoteActorSystemConfiguration;

final class ActorSystemDelegateConfiguration implements ActorSystemConfiguration {

    private final RemoteActorSystemConfiguration delegate;

    ActorSystemDelegateConfiguration(RemoteActorSystemConfiguration delegate) {
        this.delegate = delegate;
    }

    @Override
    public String getName() {
        return delegate.getName();
    }

    @Override
    public int getNumberOfShards() {
        return delegate.getNumberOfShards();
    }

    @Override
    public int getQueuesPerShard() {
        return delegate.getQueuesPerShard();
    }

    @Override
    public int getQueuesPerNode() {
        // Actor Systems don't talk to one another over node queues
        return 1;
    }

    @Override
    public String getVersion() {
        return "1.0.0";
    }

    @Override
    public <T> T getProperty(Class component, String key, Class<T> targetType) {
        throw new UnsupportedOperationException("Cannot get properties for a Remote ActorSystem");
    }

    @Override
    public <T> T getProperty(Class component, String key, Class<T> targetType, T defaultValue) {
        throw new UnsupportedOperationException("Cannot get properties for a Remote ActorSystem");
    }

    @Override
    public <T> T getRequiredProperty(Class component, String key, Class<T> targetType)
            throws IllegalStateException {
        throw new UnsupportedOperationException("Cannot get properties for a Remote ActorSystem");
    }
}
