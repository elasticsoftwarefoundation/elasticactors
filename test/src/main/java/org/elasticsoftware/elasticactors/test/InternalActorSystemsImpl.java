package org.elasticsoftware.elasticactors.test;

import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.PhysicalNode;
import org.elasticsoftware.elasticactors.cluster.*;
import org.elasticsoftware.elasticactors.serialization.MessageDeserializer;
import org.elasticsoftware.elasticactors.serialization.MessageSerializer;
import org.elasticsoftware.elasticactors.serialization.SerializationFramework;
import org.elasticsoftware.elasticactors.serialization.internal.SystemDeserializers;
import org.elasticsoftware.elasticactors.serialization.internal.SystemSerializers;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.Arrays;
import java.util.List;

/**
 * @author Joost van de Wijgerd
 */
public final class InternalActorSystemsImpl implements InternalActorSystems, ActorRefFactory {
    private final SystemSerializers systemSerializers = new SystemSerializers(this);
    private final SystemDeserializers systemDeserializers = new SystemDeserializers(this);
    @Autowired
    private ApplicationContext applicationContext;
    @Autowired
    private LocalActorSystemInstance localActorSystemInstance;
    private final PhysicalNode localNode;

    public InternalActorSystemsImpl(PhysicalNode localNode) {
        this.localNode = localNode;
    }

    @PostConstruct
    public void initialize() throws Exception {
        final List<PhysicalNode> localNodes = Arrays.<PhysicalNode>asList(localNode);
        localActorSystemInstance.updateNodes(localNodes);
        localActorSystemInstance.distributeShards(localNodes);
    }

    @PreDestroy
    public void destroy() {

    }

    @Override
    public ActorRef create(String refSpec) {
        return ActorRefTools.parse(refSpec, this);
    }

    @Override
    public String getClusterName() {
        return "testcluster";
    }

    @Override
    public InternalActorSystem get(String name) {
        return applicationContext.getBean(InternalActorSystem.class);
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
    public SerializationFramework getSerializationFramework(Class<? extends SerializationFramework> frameworkClass) {
        return applicationContext.getBean(frameworkClass);
    }
}
