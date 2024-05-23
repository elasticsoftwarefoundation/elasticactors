/*
 * Copyright 2013 - 2024 The Original Authors
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

package org.elasticsoftware.elasticactors.base.serialization;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.module.afterburner.AfterburnerModule;
import net.jodah.typetools.TypeResolver;
import org.elasticsoftware.elasticactors.cluster.ActorRefFactory;
import org.elasticsoftware.elasticactors.cluster.scheduler.ScheduledMessageRefFactory;
import org.reflections.Reflections;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;
import org.reflections.util.FilterBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * {@link com.fasterxml.jackson.annotation.JsonTypeName}. The classes are registered as subtypes
 * (see {@link com.fasterxml.jackson.databind.ObjectMapper#registerSubtypes(Class[])})
 *
 * @author Joost van de Wijgerd
 */
public class ObjectMapperBuilder {
    private static final Logger logger = LoggerFactory.getLogger(ObjectMapperBuilder.class);
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

        logger.info("Building ObjectMapper");
        ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();

        logger.debug("Scanning the following packages: {}", basePackages);

        for (String basePackage : basePackages) {
            configurationBuilder.addUrls(ClasspathHelper.forPackage(basePackage));
        }

        FilterBuilder filterBuilder = new FilterBuilder();
        filterBuilder.excludePackage(JsonSerializer.class.getPackage().getName());
        filterBuilder.excludePackage(JsonDeserializer.class.getPackage().getName());

        configurationBuilder.filterInputsBy(filterBuilder);

        Reflections reflections = new Reflections(configurationBuilder);
        registerSubtypes(reflections, objectMapper);

        SimpleModule elasticActorsModule =
            new SimpleModule("ElasticActorsModule", new Version(1, 0, 0, null, null, null));

        registerCustomSerializers(reflections,elasticActorsModule);
        registerCustomDeserializers(reflections,elasticActorsModule);

        logger.info("Registering Jackson module: {}", elasticActorsModule.getModuleName());
        objectMapper.registerModule(elasticActorsModule);

        if(useAfterBurner) {
            // register the afterburner module to increase performance
            // afterburner does NOT work correctly with jackson version lower than 2.4.5! (if @JsonSerialize annotation is used)
            AfterburnerModule afterburnerModule = new AfterburnerModule();
            logger.info("Registering Jackson module: {}", afterburnerModule.getModuleName());
            //afterburnerModule.setUseValueClassLoader(false);
            //afterburnerModule.setUseOptimizedBeanDeserializer(false);
            objectMapper.registerModule(afterburnerModule);
        }

        return objectMapper;
    }

    private void registerSubtypes(Reflections reflections,ObjectMapper objectMapper) {
        logger.info("Scanning @JsonTypeName-annotated classes");
        Set<Class<?>> subTypes = reflections.getTypesAnnotatedWith(JsonTypeName.class);
        logger.info("Found {} classes annotated with @JsonTypeName", subTypes.size());
        if (logger.isDebugEnabled()) {
            logger.debug(
                "Found the following classes annotated with @JsonTypeName: {}",
                subTypes.stream().map(Class::getName).collect(Collectors.toList())
            );
        }
        objectMapper.registerSubtypes(subTypes.toArray(new Class<?>[0]));
    }

    private void registerCustomSerializers(Reflections reflections, SimpleModule jacksonModule) {
        logger.info("Scanning classes that extend JsonSerializer");
        List<Class<? extends JsonSerializer>> customSerializers =
            reflections.getSubTypesOf(JsonSerializer.class)
                .stream()
                .filter(ObjectMapperBuilder::isInstantiable)
                .filter(ObjectMapperBuilder::hasNoArgConstructor)
                .collect(Collectors.toList());
        logger.info("Found {} classes that extend JsonSerializer", customSerializers.size());
        if (logger.isDebugEnabled()) {
            logger.debug(
                "Found the following classes extending JsonSerializer: {}",
                customSerializers.stream().map(Class::getName).collect(Collectors.toList())
            );
        }
        for (Class<? extends JsonSerializer> customSerializer : customSerializers) {
            try {
                JsonSerializer jsonSerializer = customSerializer.newInstance();
                Class<?> objectClass = resolveSerializerHandledType(customSerializer, jsonSerializer);
                logger.debug(
                    "Adding serializer [{}] for type [{}]",
                    customSerializer.getName(),
                    objectClass.getName()
                );
                jacksonModule.addSerializer(objectClass, jsonSerializer);
            } catch (Exception e) {
                logger.error(
                    "Failed to create Custom Jackson Serializer: {}",
                    customSerializer.getName(),
                    e
                );
            }
        }
    }

    private static Class<?> resolveSerializerHandledType(
        Class<? extends JsonSerializer> customSerializer,
        JsonSerializer jsonSerializer)
    {
        Class<?> objectClass =
            TypeResolver.resolveRawArgument(JsonSerializer.class, customSerializer);
        if (TypeResolver.Unknown.class.equals(objectClass)) {
            logger.debug(
                "Could not resolve the type handled by serializer of type [{}]. "
                    + "Trying to get it from JsonSerializer.handledType()",
                customSerializer.getName()
            );
            // Best effort. This may not always be correct.
            objectClass = jsonSerializer.handledType();
        }
        return objectClass;
    }

    private void registerCustomDeserializers(Reflections reflections, SimpleModule jacksonModule) {
        logger.info("Scanning classes that extend JsonDeserializer");
        List<Class<? extends JsonDeserializer>> customDeserializers =
            reflections.getSubTypesOf(JsonDeserializer.class)
                .stream()
                .filter(ObjectMapperBuilder::isInstantiable)
                .filter(ObjectMapperBuilder::hasUsableDeserializerConstructor)
                .collect(Collectors.toList());
        logger.info("Found {} classes that extend JsonDeserializer", customDeserializers.size());
        if (logger.isDebugEnabled()) {
            logger.debug(
                "Found the following classes extending JsonDeserializer: {}",
                customDeserializers.stream().map(Class::getName).collect(Collectors.toList())
            );
        }
        for (Class<? extends JsonDeserializer> customDeserializer : customDeserializers) {
            // need to exclude the JacksonActorRefDeserializer
            if (hasNoArgConstructor(customDeserializer)) {
                try {
                    JsonDeserializer deserializer = customDeserializer.newInstance();
                    Class<?> objectClass =
                        resolveDeserializerHandledType(customDeserializer, deserializer);
                    logger.debug(
                        "Adding deserializer [{}] for type [{}]",
                        customDeserializer.getName(),
                        objectClass.getName()
                    );
                    jacksonModule.addDeserializer(objectClass, deserializer);
                } catch (Exception e) {
                    logger.error(
                        "Failed to create Custom Jackson Deserializer: {}",
                        customDeserializer.getName(),
                        e
                    );
                }
            } else if (hasActorRefFactoryConstructor(customDeserializer)) {
                try {
                    Constructor<? extends JsonDeserializer> constructor =
                        customDeserializer.getConstructor(ActorRefFactory.class);
                    JsonDeserializer deserializer = constructor.newInstance(actorRefFactory);
                    Class<?> objectClass =
                        resolveDeserializerHandledType(customDeserializer, deserializer);
                    logger.debug(
                        "Adding deserializer [{}] for type [{}]",
                        customDeserializer.getName(),
                        objectClass.getName()
                    );
                    jacksonModule.addDeserializer(objectClass, deserializer);
                } catch (Exception e) {
                    logger.error(
                        "Failed to create Custom Jackson Deserializer: {}",
                        customDeserializer.getName(),
                        e
                    );
                }
            } else if (hasScheduledMessageRefFactoryConstructor(customDeserializer)) {
                try {
                    Constructor<? extends JsonDeserializer> constructor =
                        customDeserializer.getConstructor(ScheduledMessageRefFactory.class);
                    JsonDeserializer deserializer =
                        constructor.newInstance(scheduledMessageRefFactory);
                    Class<?> objectClass =
                        resolveDeserializerHandledType(customDeserializer, deserializer);
                    logger.debug(
                        "Adding deserializer [{}] for type [{}]",
                        customDeserializer.getName(),
                        objectClass.getName()
                    );
                    jacksonModule.addDeserializer(objectClass, deserializer);
                } catch (Exception e) {
                    logger.error(
                        "Failed to create Custom Jackson Deserializer: {}",
                        customDeserializer.getName(),
                        e
                    );
                }
            } else {
                logger.error(
                    "Could not find a suitable constructor for deserializer [{}]",
                    customDeserializer.getName()
                );
            }
        }
    }

    private Class<?> resolveDeserializerHandledType(
        Class<? extends JsonDeserializer> customDeserializer,
        JsonDeserializer deserializer)
    {
        Class<?> objectClass =
            TypeResolver.resolveRawArgument(JsonSerializer.class, customDeserializer);
        if (TypeResolver.Unknown.class.equals(objectClass)) {
            logger.debug(
                "Could not resolve the type handled by deserializer of type [{}]. "
                    + "Trying to get it from JsonDeserializer.handledType()",
                customDeserializer.getName()
            );
            // Best effort. This may not always be correct.
            objectClass = deserializer.handledType();
        }
        return objectClass;
    }

    private static boolean hasUsableDeserializerConstructor(Class<? extends JsonDeserializer> deserializerClass) {
        return hasNoArgConstructor(deserializerClass)
            || hasActorRefFactoryConstructor(deserializerClass)
            || hasScheduledMessageRefFactoryConstructor(deserializerClass);
    }

    private static boolean hasActorRefFactoryConstructor(Class<? extends JsonDeserializer> deserializerClass) {
        return hasSingleParameterConstructorMatching(deserializerClass, ActorRefFactory.class);
    }

    private static boolean hasScheduledMessageRefFactoryConstructor(Class<? extends JsonDeserializer> deserializerClass) {
        return hasSingleParameterConstructorMatching(
            deserializerClass,
            ScheduledMessageRefFactory.class
        );
    }

    private static boolean isInstantiable(Class<?> aClass) {
        return !aClass.isInterface()
            && !aClass.isAnonymousClass()
            && !aClass.isEnum()
            && !Modifier.isAbstract(aClass.getModifiers());
    }

    private static boolean hasNoArgConstructor(Class<?> aClass) {
        try {
            aClass.getConstructor();
        } catch(NoSuchMethodException e) {
            return false;
        }
        return true;
    }

    private static boolean hasSingleParameterConstructorMatching(
        Class<?> aClass,
        Class<?> parameterClass)
    {
        try {
            aClass.getConstructor(parameterClass);
        } catch(NoSuchMethodException e) {
            return false;
        }
        return true;
    }
}

