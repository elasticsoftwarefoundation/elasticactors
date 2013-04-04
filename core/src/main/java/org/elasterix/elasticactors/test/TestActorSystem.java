/*
 * Copyright 2013 Joost van de Wijgerd
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

package org.elasterix.elasticactors.test;

import org.elasterix.elasticactors.ActorRef;
import org.elasterix.elasticactors.ActorSystem;
import org.elasterix.elasticactors.ActorSystemConfiguration;
import org.elasterix.elasticactors.PhysicalNode;
import org.elasterix.elasticactors.cluster.*;
import org.elasterix.elasticactors.serialization.MessageDeserializer;
import org.elasterix.elasticactors.serialization.MessageSerializer;
import org.elasterix.elasticactors.serialization.internal.SystemDeserializers;
import org.elasterix.elasticactors.serialization.internal.SystemSerializers;
import org.springframework.beans.factory.annotation.Configurable;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author Joost van de Wijgerd
 */
@Configurable
public class TestActorSystem implements InternalActorSystems,ActorRefFactory {
    private final SystemSerializers systemSerializers = new SystemSerializers(this);
    private final SystemDeserializers systemDeserializers = new SystemDeserializers(this);
    private final ConcurrentMap<String,LocalActorSystemInstance> actorSystemInstances = new ConcurrentHashMap<String,LocalActorSystemInstance>();
    private static final AtomicReference<ApplicationContext> applicationContextHolder = new AtomicReference<ApplicationContext>(null);

    private TestActorSystem() {

    }

    private void addActorSystemInstance(LocalActorSystemInstance instance) {
        actorSystemInstances.putIfAbsent(instance.getName(),instance);
    }

    public static ActorSystem create(ActorSystemConfiguration configuration) throws Exception {
        ApplicationContext applicationContext = applicationContextHolder.get();
        if(applicationContext == null) {
            applicationContext = new ClassPathXmlApplicationContext("local-beans.xml");
            if(!applicationContextHolder.compareAndSet(null,applicationContext)) {
                applicationContext = applicationContextHolder.get();
            }
        }
        final PhysicalNode localNode = new PhysicalNodeImpl("localnode",InetAddress.getLocalHost(),true);
        NodeSelectorFactory factory = new NodeSelectorFactory() {
            @Override
            public NodeSelector create(List<PhysicalNode> nodes) {
                return new NodeSelector() {
                    @Override
                    public List<PhysicalNode> getAll() {
                        return Arrays.asList(localNode);
                    }

                    @Override
                    public PhysicalNode getPrimary(String shardKey) {
                        return localNode;
                    }
                };
            }

            @Override
            public void start() throws Exception {
                //To change body of implemented methods use File | Settings | File Templates.
            }
        };
        TestActorSystem testActorSystem = applicationContext.getBean(TestActorSystem.class);
        LocalActorSystemInstance actorSystemInstance = new LocalActorSystemInstance(localNode,
                                                                                    testActorSystem,
                                                                                    configuration,
                                                                                    factory);
        testActorSystem.addActorSystemInstance(actorSystemInstance);
        List<PhysicalNode> nodeList = Arrays.asList(localNode);
        actorSystemInstance.updateNodes(nodeList);
        actorSystemInstance.distributeShards(nodeList);

        return actorSystemInstance;
    }

    @Override
    public ActorRef create(String refSpec) {
        return ActorRefTools.parse(refSpec,this);
    }



    @Override
    public String getClusterName() {
        return "LocalNode";
    }

    @Override
    public InternalActorSystem get(String actorSystemName) {
        return actorSystemInstances.get(actorSystemName);
    }

    @Override
    public <T> MessageSerializer<T> getSystemMessageSerializer(Class<T> messageClass) {
        return systemSerializers.get(messageClass);
    }

    @Override
    public <T> MessageDeserializer<T> getSystemMessageDeserializer(Class<T> messageClass) {
        return systemDeserializers.get(messageClass);
    }

    @Override
    public ActorRefFactory getActorRefFactory() {
        return this;
    }
}
