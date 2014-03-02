/*
 * Copyright 2013 - 2014 The Original Authors
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

package org.elasticsoftware.elasticactors.base.serialization;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdScalarDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.apache.log4j.Logger;
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.cluster.ActorRefFactory;
import org.jodah.typetools.TypeResolver;
import org.reflections.Reflections;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;

import java.io.IOException;
import java.net.URL;
import java.util.*;

/**
 * {@link com.fasterxml.jackson.annotation.JsonTypeName}. The classes are registered as subtypes
 * (see {@link com.fasterxml.jackson.databind.ObjectMapper#registerSubtypes(Class[])})
 *
 * @author Joost van de Wijgerd
 */
public class ObjectMapperBuilder {
    private static final Logger logger = Logger.getLogger(ObjectMapperBuilder.class);
    public static final String RESOURCE_NAME = "META-INF/elasticactors.properties";
    private final String version;
    private final ActorRefFactory actorRefFactory;
    private final String basePackages;

    public ObjectMapperBuilder(ActorRefFactory actorRefFactory, String version) {
        this(actorRefFactory, version, "");
    }

    public ObjectMapperBuilder(ActorRefFactory actorRefFactory, String basePackages, String version) {
        this.version = version;
        this.actorRefFactory = actorRefFactory;
        this.basePackages = basePackages;
    }

    public final ObjectMapper build() {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);

        // scan everything for META-INF/elasticactors.properties
        Set<String> basePackages = new HashSet<>();
        try {
            Enumeration<URL> resources = Thread.currentThread().getContextClassLoader().getResources(RESOURCE_NAME);
            while (resources.hasMoreElements()) {
                URL url = resources.nextElement();
                Properties props = new Properties();
                props.load(url.openStream());
                basePackages.add(props.getProperty("basePackage"));
            }
        } catch(IOException e) {
            logger.warn(String.format("Failed to load elasticactors.properties"),e);
        }

        // add other base packages
        if(this.basePackages != null && !"".equals(this.basePackages)) {
            String[] otherPackages = this.basePackages.split(",");
            basePackages.addAll(Arrays.asList(otherPackages));
        }

        ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();

        for (String basePackage : basePackages) {
            configurationBuilder.addUrls(ClasspathHelper.forPackage(basePackage));
        }

        Reflections reflections = new Reflections(configurationBuilder);
        registerSubtypes(reflections, objectMapper);

        SimpleModule jacksonModule = new SimpleModule("org.elasticsoftware.elasticactors",new Version(1,0,0,null,null,null));

        registerCustomSerializers(reflections,jacksonModule);
        registerCustomDeserializers(reflections,jacksonModule);

        objectMapper.registerModule(jacksonModule);

        return objectMapper;
    }

    private void registerSubtypes(Reflections reflections,ObjectMapper objectMapper) {
        Set<Class<?>> subTypes = reflections.getTypesAnnotatedWith(JsonTypeName.class);
        objectMapper.registerSubtypes(subTypes.toArray(new Class<?>[subTypes.size()]));
    }

    private void registerCustomSerializers(Reflections reflections, SimpleModule jacksonModule) {
        Set<Class<? extends JsonSerializer>> customSerializers = reflections.getSubTypesOf(JsonSerializer.class);
        for (Class<? extends JsonSerializer> customSerializer : customSerializers) {
            Class<?> objectClass = TypeResolver.resolveRawArgument(JsonSerializer.class,customSerializer);
            try {
                jacksonModule.addSerializer(objectClass,customSerializer.newInstance());
            } catch(Exception e) {
                logger.warn(String.format("Failed to create Custom Jackson Serializer: %s",customSerializer.getName()),e);
            }
        }
    }

    private void registerCustomDeserializers(Reflections reflections, SimpleModule jacksonModule) {
        // @todo: looks like the SubTypeScanner only goes one level deep, need to fix this
        Set<Class<? extends StdScalarDeserializer>> customDeserializers = reflections.getSubTypesOf(StdScalarDeserializer.class);
        for (Class<? extends StdScalarDeserializer> customDeserializer : customDeserializers) {
            // need to exclude the JacksonActorRefDeserializer
            if(!JacksonActorRefDeserializer.class.equals(customDeserializer)) {
                // @todo: figure out which method is faster
                //Class<?> objectClass = TypeResolver.resolveRawArgument(StdScalarDeserializer.class, customDeserializer);
                try {
                    StdScalarDeserializer deserializer = customDeserializer.newInstance();
                    Class<?> objectClass = deserializer.handledType();
                    jacksonModule.addDeserializer(objectClass, deserializer);
                } catch(Exception e) {
                    logger.warn(String.format("Failed to create Custom Jackson Deserializer: %s", customDeserializer.getName()),e);
                }
            } else {
                // this one can currently not be created by the scanner due to the special constructor
                jacksonModule.addDeserializer(ActorRef.class, new JacksonActorRefDeserializer(actorRefFactory));
            }
        }
    }
}

