/*
 * Copyright 2013 the original authors
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

package org.elasticsoftware.elasticactors.base;

import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.elasticsoftware.elasticactors.*;
import org.elasticsoftware.elasticactors.base.serialization.JacksonMessageDeserializer;
import org.elasticsoftware.elasticactors.base.serialization.JacksonMessageSerializer;
import org.elasticsoftware.elasticactors.serialization.Deserializer;
import org.elasticsoftware.elasticactors.serialization.MessageDeserializer;
import org.elasticsoftware.elasticactors.serialization.MessageSerializer;
import org.elasticsoftware.elasticactors.serialization.Serializer;
import org.elasticsoftware.elasticactors.base.serialization.JacksonActorRefDeserializer;
import org.elasticsoftware.elasticactors.base.serialization.JacksonActorRefSerializer;
import org.springframework.beans.BeansException;
import org.springframework.beans.MutablePropertyValues;
import org.springframework.beans.PropertyValue;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.config.PropertyResourceConfigurer;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.BeanDefinitionRegistryPostProcessor;
import org.springframework.beans.factory.support.GenericBeanDefinition;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author Joost van de Wijgerd
 */
public abstract class SpringBasedActorSystem implements ActorSystemConfiguration, ActorSystemBootstrapper {
    public static final String DESERIALIZERS = "messageDeserializers";
    public static final String SERIALIZERS = "messageSerializers";
    public static final String STATE_SERIALIZER = "actorStateSerializer";
    public static final String STATE_DESERIALIZER = "actorStateDeserializer";
    private final String[] contextConfigLocations;
    private ConfigurableApplicationContext applicationContext;
    private final ConcurrentMap<Class,JacksonMessageSerializer> cachedSerializers = new ConcurrentHashMap<Class,JacksonMessageSerializer>();
    private final ConcurrentMap<Class,JacksonMessageDeserializer> cachedDeserializers = new ConcurrentHashMap<Class,JacksonMessageDeserializer>();

    protected SpringBasedActorSystem(String... contextConfigLocations) {
        this.contextConfigLocations = new String[contextConfigLocations.length+1];
        this.contextConfigLocations[0] = "base-beans.xml";
        System.arraycopy(contextConfigLocations, 0, this.contextConfigLocations, 1, contextConfigLocations.length);
    }

    @Override
    public final void initialize(ActorSystem actorSystem, final Properties properties) throws Exception {
        applicationContext = new ClassPathXmlApplicationContext(contextConfigLocations,false,null);
        applicationContext.addBeanFactoryPostProcessor(new BeanDefinitionRegistryPostProcessor() {
            @Override
            public void postProcessBeanDefinitionRegistry(BeanDefinitionRegistry registry) throws BeansException {
                GenericBeanDefinition beanDefinition = new GenericBeanDefinition();
                beanDefinition.setBeanClass(PropertySourcesPlaceholderConfigurer.class);
                beanDefinition.setPropertyValues(new MutablePropertyValues(Arrays.asList(new PropertyValue("properties",properties))));
                registry.registerBeanDefinition("propertyPlaceholder",beanDefinition);
            }

            @Override
            public void postProcessBeanFactory(ConfigurableListableBeanFactory beanFactory) throws BeansException {
                //To change body of implemented methods use File | Settings | File Templates.
            }
        });
        applicationContext.refresh();


        ObjectMapper objectMapper = applicationContext.getBean(ObjectMapper.class);
        // register jackson module for Actor ref ser/de
        objectMapper.registerModule(
                new SimpleModule("ElasticActorsModule",new Version(0,1,0,"SNAPSHOT"))
                .addSerializer(ActorRef.class, new JacksonActorRefSerializer())
                .addDeserializer(ActorRef.class, new JacksonActorRefDeserializer(actorSystem.getParent().getActorRefFactory())));

        doInitialize(applicationContext, actorSystem);
    }

    protected abstract void doInitialize(ApplicationContext applicationContext, ActorSystem actorSystem);

    @Override
    public void create(ActorSystem actorSystem, String... arguments) throws Exception {

    }

    @Override
    public void activate(ActorSystem actorSystem) throws Exception {

    }

    @Override
    public void destroy() throws Exception {
        applicationContext.close();
    }

    @Override
    public String getVersion() {
        String version = getClass().getPackage().getImplementationVersion();
        return (version != null) ? version : "UNKNOWN";
    }

    @Override
    public final <T> MessageSerializer<T> getSerializer(Class<T> messageClass) {
        MessageSerializer<T> messageSerializer = null;
        // first see if we have one wire up
        if(applicationContext.containsBean(SERIALIZERS)) {
            Map<Class,MessageSerializer> messageSerializers = (Map<Class, MessageSerializer>) applicationContext.getBean(SERIALIZERS);
            messageSerializer = messageSerializers.get(messageClass);
        }
        if(messageSerializer == null) {
            // default to jackson
            if(!cachedSerializers.containsKey(messageClass)) {
                //@todo: somehow check if this messageClass can be serialized by jackson
                cachedSerializers.putIfAbsent(messageClass,new JacksonMessageSerializer(applicationContext.getBean(ObjectMapper.class)));
            }
            messageSerializer = cachedSerializers.get(messageClass);
        }
        return messageSerializer;
    }

    @Override
    public final <T> MessageDeserializer<T> getDeserializer(Class<T> messageClass) {
        MessageDeserializer<T> messageDeserializer = null;
        if(applicationContext.containsBean(DESERIALIZERS)) {
            Map<Class,MessageDeserializer> messageDeserializers = (Map<Class, MessageDeserializer>) applicationContext.getBean(DESERIALIZERS);
            messageDeserializer = messageDeserializers.get(messageClass);
        }
        if(messageDeserializer == null) {
            // default to jackson
            if(!cachedDeserializers.containsKey(messageClass)) {
                //@todo: somehow check if this messageClass can be deserialized by jackson
                cachedDeserializers.putIfAbsent(messageClass,new JacksonMessageDeserializer(messageClass,applicationContext.getBean(ObjectMapper.class)));
            }
            messageDeserializer = cachedDeserializers.get(messageClass);
        }
        return messageDeserializer;
    }

    @Override
    public final Serializer<ActorState, byte[]> getActorStateSerializer() {
        return applicationContext.getBean(STATE_SERIALIZER,Serializer.class);
    }

    @Override
    public final Deserializer<byte[], ActorState> getActorStateDeserializer() {
        return applicationContext.getBean(STATE_DESERIALIZER,Deserializer.class);
    }

    @Override
    public ActorStateFactory getActorStateFactory() {
        return applicationContext.getBean(ActorStateFactory.class);
    }

    @Override
    public final ElasticActor getService(String serviceId) {
        return applicationContext.getBean(serviceId,ElasticActor.class);
    }

    @Override
    public final Set<String> getServices() {
        return applicationContext.getBeansOfType(ElasticActor.class).keySet();
    }
}
