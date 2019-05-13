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

package org.elasticsoftware.elasticactors.base.serialization;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdScalarDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.module.afterburner.AfterburnerModule;
import net.jodah.typetools.TypeResolver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsoftware.elasticactors.cluster.ActorRefFactory;
import org.elasticsoftware.elasticactors.cluster.scheduler.ScheduledMessageRefFactory;
import org.reflections.Reflections;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.URL;
import java.util.*;

/**
 * {@link com.fasterxml.jackson.annotation.JsonTypeName}. The classes are registered as subtypes
 * (see {@link com.fasterxml.jackson.databind.ObjectMapper#registerSubtypes(Class[])})
 *
 * @author Joost van de Wijgerd
 */
public class ObjectMapperBuilder {
    private static final Logger logger = LogManager.getLogger(ObjectMapperBuilder.class);
    public static final String RESOURCE_NAME = "META-INF/elasticactors.properties";
    private final String version;
    private final ActorRefFactory actorRefFactory;
    private final ScheduledMessageRefFactory scheduledMessageRefFactory;
    private final String basePackages;
    private boolean useAfterBurner = false;

    public ObjectMapperBuilder(ActorRefFactory actorRefFactory,ScheduledMessageRefFactory scheduledMessageRefFactory, String version) {
        this(actorRefFactory,scheduledMessageRefFactory, "",version);
    }

    public ObjectMapperBuilder(ActorRefFactory actorRefFactory,ScheduledMessageRefFactory scheduledMessageRefFactory, String basePackages, String version) {
        this.version = version;
        this.actorRefFactory = actorRefFactory;
        this.scheduledMessageRefFactory = scheduledMessageRefFactory;
        this.basePackages = basePackages;
    }

    public ObjectMapperBuilder setUseAfterBurner(boolean useAfterBurner) {
        this.useAfterBurner = useAfterBurner;
        return this;
    }

    public final ObjectMapper build() {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

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
            logger.warn("Failed to load elasticactors.properties", e);
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

        if(useAfterBurner) {
            // register the afterburner module to increase performance
            // afterburner does NOT work correctly with jackson version lower than 2.4.5! (if @JsonSerialize annotation is used)
            AfterburnerModule afterburnerModule = new AfterburnerModule();
            //afterburnerModule.setUseValueClassLoader(false);
            //afterburnerModule.setUseOptimizedBeanDeserializer(false);
            objectMapper.registerModule(afterburnerModule);
        }

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
                jacksonModule.addSerializer(objectClass, customSerializer.newInstance());
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
            if(hasNoArgConstructor(customDeserializer)) {
                try {
                    StdScalarDeserializer deserializer = customDeserializer.newInstance();
                    Class<?> objectClass = deserializer.handledType();
                    jacksonModule.addDeserializer(objectClass, deserializer);
                } catch(Exception e) {
                    logger.error(String.format("Failed to create Custom Jackson Deserializer: %s", customDeserializer.getName()), e);
                }
            } else {
                // this ones can currently not be created by the scanner due to the special constructor
                for (Constructor<?> constructor : customDeserializer.getConstructors()) {
                    if(hasSingleConstrutorParameterMatching(constructor,ActorRefFactory.class)) {
                        try {
                            StdScalarDeserializer deserializer = (StdScalarDeserializer) constructor.newInstance(actorRefFactory);
                            Class<?> objectClass = deserializer.handledType();
                            jacksonModule.addDeserializer(objectClass, deserializer);
                            break;
                        } catch(Exception e) {
                            logger.error(String.format("Failed to create Custom Jackson Deserializer: %s", customDeserializer.getName()),e);
                        }
                    } else if(hasSingleConstrutorParameterMatching(constructor,ScheduledMessageRefFactory.class)) {
                        try {
                            StdScalarDeserializer deserializer = (StdScalarDeserializer) constructor.newInstance(scheduledMessageRefFactory);
                            Class<?> objectClass = deserializer.handledType();
                            jacksonModule.addDeserializer(objectClass, deserializer);
                            break;
                        } catch(Exception e) {
                            logger.error(String.format("Failed to create Custom Jackson Deserializer: %s", customDeserializer.getName()),e);
                        }
                    }
                }

            }
        }
    }

    private boolean hasNoArgConstructor(Class<? extends StdScalarDeserializer> customDeserializer) {
        try {
            customDeserializer.getConstructor();
        } catch(NoSuchMethodException e) {
            return false;
        }
        return true;
    }

    private boolean hasSingleConstrutorParameterMatching(Constructor constructor,Class parameterClass) {
        if(constructor.getParameterTypes().length == 1) {
            return constructor.getParameterTypes()[0].equals(parameterClass);
        } else {
            return false;
        }
    }
}

