package org.elasticsoftware.elasticactors.runtime;

import org.apache.log4j.Logger;
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

import java.net.InetAddress;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * @author Joost van de Wijgerd
 */
public class ElasticActorsNode implements PhysicalNode, InternalActorSystems, ActorRefFactory {
    private static final Logger logger = Logger.getLogger(ElasticActorsNode.class);
    private final String clusterName;
    private final String nodeId;
    private final InetAddress nodeAddress;
    private final SystemSerializers systemSerializers = new SystemSerializers(this);
    private final SystemDeserializers systemDeserializers = new SystemDeserializers(this);
    private final CountDownLatch waitLatch = new CountDownLatch(1);
    @Autowired
    private ApplicationContext applicationContext;


    public ElasticActorsNode(String clusterName, String nodeId, InetAddress nodeAddress) {
        this.clusterName = clusterName;
        this.nodeId = nodeId;
        this.nodeAddress = nodeAddress;
    }

    public void join() {
        //@todo: remove this once clustering is supported!
        List<PhysicalNode> clusterNodes = Arrays.asList((PhysicalNode)this);
        LocalActorSystemInstance instance = applicationContext.getBean(LocalActorSystemInstance.class);
        logger.info(String.format("Updating %d nodes for ActorSystem[%s]", clusterNodes.size(), instance.getName()));
        try {
            instance.updateNodes(clusterNodes);
        } catch (Exception e) {
            logger.error(String.format("ActorSystem[%s] failed to update nodes", instance.getName()), e);
        }
        logger.info(String.format("Rebalancing %d shards for ActorSystem[%s]", instance.getNumberOfShards(), instance.getName()));
        try {
            instance.distributeShards(clusterNodes);
        } catch (Exception e) {
            logger.error(String.format("ActorSystem[%s] failed to (re-)distribute shards", instance.getName()), e);
        }

        try {
            waitLatch.await();
        } catch (InterruptedException e) {
            //
        }
    }

    @Override
    public ActorRef create(String refSpec) {
        return ActorRefTools.parse(refSpec, this);
    }

    @Override
    public String getClusterName() {
        return clusterName;
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

    @Override
    public boolean isLocal() {
        return true;
    }

    @Override
    public String getId() {
        return nodeId;
    }

    @Override
    public InetAddress getAddress() {
        return nodeAddress;
    }


}
