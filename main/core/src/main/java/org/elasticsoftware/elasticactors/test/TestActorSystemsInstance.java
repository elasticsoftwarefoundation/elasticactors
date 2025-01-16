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

package org.elasticsoftware.elasticactors.test;

import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.ActorSystem;
import org.elasticsoftware.elasticactors.ActorSystems;
import org.elasticsoftware.elasticactors.cluster.ActorRefFactory;
import org.elasticsoftware.elasticactors.cluster.RebalancingEventListener;
import org.elasticsoftware.elasticactors.serialization.MessageDeserializer;
import org.elasticsoftware.elasticactors.serialization.MessageSerializer;
import org.elasticsoftware.elasticactors.serialization.SerializationFramework;

/**
 * @author Joost van de Wijgerd
 */
public class TestActorSystemsInstance implements ActorSystems,ActorRefFactory {
    @Override
    public ActorRef create(String refSpec) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public ActorSystem getRemote(String clusterName, String actorSystemName) {
        return null;  
    }

    @Override
    public ActorSystem getRemote(String actorSystemName) {
        return null;
    }

    @Override
    public String getClusterName() {
        return "local";
    }

    @Override
    public ActorSystem get(String actorSystemName) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void registerRebalancingEventListener(RebalancingEventListener eventListener) {
        // silently ignore
    }

    public <T> MessageSerializer<T> getSystemMessageSerializer(Class<T> messageClass) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public <T> MessageDeserializer<T> getSystemMessageDeserializer(Class<T> messageClass) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public SerializationFramework getSerializationFramework(Class<? extends SerializationFramework> frameworkClass) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }
}
