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

import org.elasterix.elasticactors.*;
import org.elasterix.elasticactors.cluster.*;
import org.elasterix.elasticactors.serialization.Deserializer;
import org.elasterix.elasticactors.serialization.MessageDeserializer;
import org.elasterix.elasticactors.serialization.MessageSerializer;
import org.elasterix.elasticactors.serialization.Serializer;
import org.elasterix.elasticactors.serialization.internal.*;
import org.springframework.beans.factory.annotation.Configurable;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.List;

/**
 * @author Joost van de Wijgerd
 */
@Configurable
public class TestActorSystem implements InternalActorSystems,ActorRefFactory {
    private final SystemSerializers systemSerializers = new SystemSerializers(this);
    private final SystemDeserializers systemDeserializers = new SystemDeserializers(this);
    private LocalActorSystemInstance actorSystemInstance;

    private TestActorSystem() {

    }

    private void setActorSystemInstance(LocalActorSystemInstance actorSystemInstance) {
        this.actorSystemInstance = actorSystemInstance;
    }

    public static ActorSystem create(ActorSystemConfiguration configuration) throws Exception {
        ClassPathXmlApplicationContext applicationContext = new ClassPathXmlApplicationContext("local-beans.xml");
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
        ActorSystemConfiguration wrapper = (configuration instanceof ActorSystemBootstrapper) ?
                                            new ActorSystemConfigurationAndBootstrapperWrapper(configuration,(ActorSystemBootstrapper)configuration)
                                            : new ActorSystemConfigurationWrapper(configuration);
        LocalActorSystemInstance actorSystemInstance = new LocalActorSystemInstance(testActorSystem,
                                                                                    wrapper,
                                                                                    factory);
        testActorSystem.setActorSystemInstance(actorSystemInstance);
        actorSystemInstance.distributeShards(Arrays.asList(localNode));

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
        return actorSystemInstance;
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

    private static final class ActorSystemConfigurationAndBootstrapperWrapper implements ActorSystemConfiguration,ActorSystemBootstrapper {
        private final ActorSystemConfiguration configuration;
        private final ActorSystemBootstrapper bootstrapper;

        private ActorSystemConfigurationAndBootstrapperWrapper(ActorSystemConfiguration configuration, ActorSystemBootstrapper bootstrapper) {
            this.configuration = configuration;
            this.bootstrapper = bootstrapper;
        }


        @Override
        public String getName() {
            return configuration.getName();
        }

        @Override
        public int getNumberOfShards() {
            return 1;
        }

        @Override
        public String getVersion() {
            return configuration.getVersion();
        }

        @Override
        public <T> MessageSerializer<T> getSerializer(Class<T> messageClass) {
            return configuration.getSerializer(messageClass);
        }

        @Override
        public <T> MessageDeserializer<T> getDeserializer(Class<T> messageClass) {
            return configuration.getDeserializer(messageClass);
        }

        @Override
        public Serializer<ActorState, byte[]> getActorStateSerializer() {
            return configuration.getActorStateSerializer();
        }

        @Override
        public Deserializer<byte[], ActorState> getActorStateDeserializer() {
            return configuration.getActorStateDeserializer();
        }

        @Override
        public void initialize(ActorSystem actorSystem) throws Exception {
            bootstrapper.initialize(actorSystem);
        }

        @Override
        public void create(ActorSystem actorSystem, String... arguments) throws Exception {
            bootstrapper.create(actorSystem,arguments);
        }

        @Override
        public void activate(ActorSystem actorSystem) throws Exception {
            bootstrapper.activate(actorSystem);
        }
    }

    private static final class ActorSystemConfigurationWrapper implements ActorSystemConfiguration {
        private final ActorSystemConfiguration configuration;

        private ActorSystemConfigurationWrapper(ActorSystemConfiguration configuration) {
            this.configuration = configuration;
        }


        @Override
        public String getName() {
            return configuration.getName();
        }

        @Override
        public int getNumberOfShards() {
            return 1;
        }

        @Override
        public String getVersion() {
            return configuration.getVersion();
        }

        @Override
        public <T> MessageSerializer<T> getSerializer(Class<T> messageClass) {
            return configuration.getSerializer(messageClass);
        }

        @Override
        public <T> MessageDeserializer<T> getDeserializer(Class<T> messageClass) {
            return configuration.getDeserializer(messageClass);
        }

        @Override
        public Serializer<ActorState, byte[]> getActorStateSerializer() {
            return configuration.getActorStateSerializer();
        }

        @Override
        public Deserializer<byte[], ActorState> getActorStateDeserializer() {
            return configuration.getActorStateDeserializer();
        }
    }
}
