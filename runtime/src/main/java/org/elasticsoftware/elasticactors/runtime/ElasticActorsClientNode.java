package org.elasticsoftware.elasticactors.runtime;

import com.google.common.cache.Cache;
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.ActorSystemClient;
import org.elasticsoftware.elasticactors.ActorSystemClients;
import org.elasticsoftware.elasticactors.client.ClientActorRefTools;
import org.elasticsoftware.elasticactors.cluster.ActorRefFactory;
import org.elasticsoftware.elasticactors.cluster.MessageSerializationRegistry;
import org.elasticsoftware.elasticactors.cluster.SerializationFrameworkRegistry;
import org.elasticsoftware.elasticactors.serialization.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;

import java.util.HashMap;
import java.util.Map;

public class ElasticActorsClientNode implements ActorSystemClients, MessageSerializationRegistry, ActorRefFactory, SerializationFrameworkRegistry {

    private final SystemSerializers systemSerializers = new SystemSerializers(this, this);
    private final SystemDeserializers systemDeserializers;
    private final Map<Class<? extends SerializationFramework>,SerializationFramework> serializationFrameworks = new HashMap<>();
    @Autowired
    private ApplicationContext applicationContext;
    private final Cache<String,ActorRef> actorRefCache;
    private final ClientActorRefTools actorRefTools;

    @Override
    public ActorSystemClient getActorSystemClient(String clusterName, String actorSystemName) {
        return null;
    }

    public ElasticActorsClientNode(Cache<String, ActorRef> actorRefCache) {
        this.actorRefCache = actorRefCache;
        this.systemDeserializers = new SystemDeserializers(this, this,this);
        this.actorRefTools = new ClientActorRefTools(this);
    }

    @Override
    public <T> MessageSerializer<T> getSerializer(Class<T> messageClass) {
        MessageSerializer<T> messageSerializer = getSystemMessageSerializer(messageClass);
        if(messageSerializer == null) {
            Message messageAnnotation = messageClass.getAnnotation(Message.class);
            if(messageAnnotation != null) {
                SerializationFramework framework = getSerializationFramework(messageAnnotation.serializationFramework());
                messageSerializer = framework.getSerializer(messageClass);
            }
        }
        return messageSerializer;
    }

    @Override
    public <T> MessageDeserializer<T> getDeserializer(Class<T> messageClass) {
        MessageDeserializer<T> messageDeserializer = getSystemMessageDeserializer(messageClass);
        if(messageDeserializer == null) {
            Message messageAnnotation = messageClass.getAnnotation(Message.class);
            if(messageAnnotation != null) {
                SerializationFramework framework = getSerializationFramework(messageAnnotation.serializationFramework());
                messageDeserializer = framework.getDeserializer(messageClass);
            }
        }
        return messageDeserializer;
    }

    public <T> MessageSerializer<T> getSystemMessageSerializer(Class<T> messageClass) {
        return systemSerializers.get(messageClass);
    }

    public <T> MessageDeserializer<T> getSystemMessageDeserializer(Class<T> messageClass) {
        return systemDeserializers.get(messageClass);
    }

    @Override
    public SerializationFramework getSerializationFramework(Class<? extends SerializationFramework> frameworkClass) {
        //return applicationContext.getBean(frameworkClass);
        // cache the serialization frameworks for quick lookup (application context lookup is sloooooowwwwww)
        SerializationFramework serializationFramework = this.serializationFrameworks.get(frameworkClass);
        if(serializationFramework == null) {
            serializationFramework = applicationContext.getBean(frameworkClass);
            // @todo: this is not thread safe and should happen at the initialization stage
            this.serializationFrameworks.put(frameworkClass,serializationFramework);
        }
        return serializationFramework;
    }

    @Override
    public ActorRef create(final String refSpec) {
        ActorRef actorRef = actorRefCache.getIfPresent(refSpec);
        if(actorRef == null) {
            actorRef = actorRefTools.parse(refSpec);
            actorRefCache.put(refSpec,actorRef);
        }
        return actorRef;
    }
}
