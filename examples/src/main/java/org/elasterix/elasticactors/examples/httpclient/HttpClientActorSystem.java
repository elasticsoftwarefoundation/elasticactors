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

package org.elasterix.elasticactors.examples.httpclient;

import org.codehaus.jackson.Version;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.module.SimpleModule;
import org.elasterix.elasticactors.*;
import org.elasterix.elasticactors.serialization.Deserializer;
import org.elasterix.elasticactors.serialization.MessageDeserializer;
import org.elasterix.elasticactors.serialization.MessageSerializer;
import org.elasterix.elasticactors.serialization.Serializer;
import org.elasticsoftwarefoundation.elasticactors.base.serialization.JacksonActorRefDeserializer;
import org.elasticsoftwarefoundation.elasticactors.base.serialization.JacksonActorRefSerializer;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.util.Map;
import java.util.Set;

/**
 * @author Joost van de Wijgerd
 */
public class HttpClientActorSystem implements ActorSystemConfiguration, ActorSystemBootstrapper {
    private ApplicationContext applicationContext;

    @Override
    public void initialize(ActorSystem actorSystem) throws Exception {
        applicationContext = new ClassPathXmlApplicationContext("httpclient-beans.xml");
        ObjectMapper objectMapper = applicationContext.getBean(ObjectMapper.class);
        // register jackson module for Actor ref ser/de
        objectMapper.registerModule(
                new SimpleModule("ElasticActorsModule",new Version(0,1,0,"SNAPSHOT"))
                .addSerializer(ActorRef.class, new JacksonActorRefSerializer())
                .addDeserializer(ActorRef.class, new JacksonActorRefDeserializer(actorSystem.getParent().getActorRefFactory())));
    }

    @Override
    public void create(ActorSystem actorSystem, String... arguments) throws Exception {

    }

    @Override
    public void activate(ActorSystem actorSystem) throws Exception {

    }

    @Override
    public String getName() {
        return "HttpClient";
    }

    @Override
    public int getNumberOfShards() {
        return 8;
    }

    @Override
    public String getVersion() {
        return getClass().getPackage().getImplementationVersion();
    }

    @Override
    public <T> MessageSerializer<T> getSerializer(Class<T> messageClass) {
        Map<Class,MessageSerializer> messageSerializers = (Map<Class, MessageSerializer>) applicationContext.getBean("messageSerializers");
        return messageSerializers.get(messageClass);
    }

    @Override
    public <T> MessageDeserializer<T> getDeserializer(Class<T> messageClass) {
        Map<Class,MessageDeserializer> messageDeserializers = (Map<Class, MessageDeserializer>) applicationContext.getBean("messageDeserializers");
        return messageDeserializers.get(messageClass);
    }

    @Override
    public Serializer<ActorState, byte[]> getActorStateSerializer() {
        return applicationContext.getBean("actorStateSerializer",Serializer.class);
    }

    @Override
    public Deserializer<byte[], ActorState> getActorStateDeserializer() {
        return applicationContext.getBean("actorStateDeserializer",Deserializer.class);
    }

    @Override
    public ActorStateFactory getActorStateFactory() {
        return applicationContext.getBean(ActorStateFactory.class);
    }

    @Override
    public ElasticActor getService(String serviceId) {
        return applicationContext.getBean(serviceId,ElasticActor.class);
    }

    @Override
    public Set<String> getServices() {
        return applicationContext.getBeansOfType(ElasticActor.class).keySet();
    }
}
