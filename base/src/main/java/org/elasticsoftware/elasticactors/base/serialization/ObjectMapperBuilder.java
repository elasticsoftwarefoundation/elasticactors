package org.elasticsoftware.elasticactors.base.serialization;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.deser.std.StdScalarDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.apache.log4j.Logger;
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.cluster.ActorRefFactory;
import org.jodah.typetools.TypeResolver;
import org.reflections.Reflections;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;

import java.io.File;
import java.io.FileInputStream;
import java.net.URI;
import java.net.URL;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

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

    public ObjectMapperBuilder(ActorRefFactory actorRefFactory, String version) {
        this.actorRefFactory = actorRefFactory;
        this.version = version;
    }

    public final ObjectMapper build() {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);

        // scan everything for META-INF/elasticactors.properties
        Set<String> basePackages = new HashSet<>();
        Set<URL> resources = ClasspathHelper.forResource(RESOURCE_NAME);
        for (URL resource : resources) {
            try {
                Properties props = new Properties();
                props.load(new FileInputStream(new File(new URI(resource.toString()+RESOURCE_NAME))));
                basePackages.add(props.getProperty("basePackage"));
            } catch(Exception e) {
                logger.warn(String.format("Failed to load elasticactors.properties from URL: %s",resource.toString()),e);
            }
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
        for (Class<? extends JsonDeserializer> customDeserializer : customDeserializers) {
            // need to exclude the JacksonActorRefDeserializer
            if(!JacksonActorRefDeserializer.class.equals(customDeserializer)) {
                Class<?> objectClass = TypeResolver.resolveRawArgument(JsonSerializer.class, customDeserializer);
                try {
                    jacksonModule.addDeserializer(objectClass, customDeserializer.newInstance());
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

