package org.elasticsoftware.elasticactors.runtime;

import com.sun.enterprise.ee.cms.core.*;
import com.sun.enterprise.ee.cms.impl.client.*;
import com.sun.enterprise.mgmt.transport.grizzly.GrizzlyConfigConstants;
import org.apache.log4j.Logger;
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.PhysicalNode;
import org.elasticsoftware.elasticactors.cluster.*;
import org.elasticsoftware.elasticactors.serialization.MessageDeserializer;
import org.elasticsoftware.elasticactors.serialization.MessageSerializer;
import org.elasticsoftware.elasticactors.serialization.SerializationFramework;
import org.elasticsoftware.elasticactors.serialization.internal.SystemDeserializers;
import org.elasticsoftware.elasticactors.serialization.internal.SystemSerializers;
import org.elasticsoftware.elasticactors.util.concurrent.DaemonThreadFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.IOException;
import java.io.Serializable;
import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author Joost van de Wijgerd
 */
public final class ElasticActorsNode implements PhysicalNode, InternalActorSystems, ActorRefFactory {
    private static final Logger logger = Logger.getLogger(ElasticActorsNode.class);
    private final String clusterName;
    private final String nodeId;
    private final InetAddress nodeAddress;
    private final SystemSerializers systemSerializers = new SystemSerializers(this);
    private final SystemDeserializers systemDeserializers = new SystemDeserializers(this);
    private final CountDownLatch waitLatch = new CountDownLatch(1);
    @Autowired
    private ApplicationContext applicationContext;
    private GroupManagementService gms;

    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new DaemonThreadFactory("CLUSTER_SCHEDULER"));


    public ElasticActorsNode(String clusterName, String nodeId, InetAddress nodeAddress) {
        this.clusterName = clusterName;
        this.nodeId = nodeId;
        this.nodeAddress = nodeAddress;
    }

    @PostConstruct
    public void init() throws GMSException {
        gms = initializeGMS(nodeId, clusterName, nodeAddress.getHostAddress());
        gms.join();
        gms.updateMemberDetails(nodeId,"address",nodeAddress.getHostAddress());
    }

    @PreDestroy
    public void destroy() {
        reportPlannedShutdown();
        waitLatch.countDown();
    }

    private Map<Serializable, Serializable> getMemberDetails(String memberToken) {
        return gms.getMemberDetails(memberToken);
    }

    private GroupManagementService initializeGMS(String serverName,String groupName, String interfaceName) throws GMSException {
        Properties props = new Properties();

        props.setProperty(ServiceProviderConfigurationKeys.MULTICASTADDRESS.toString(),"229.9.1.1");
        props.setProperty(GrizzlyConfigConstants.BIND_INTERFACE_NAME.toString(),interfaceName);

        GroupManagementService gms =
                (GroupManagementService) GMSFactory.startGMSModule(serverName,groupName,GroupManagementService.MemberType.CORE,props);


        final CallBack gmsCallback = new CallBack() {
            @Override
            public void processNotification(Signal notification) {
                logger.info(String.format("got signal [%s] from member [%s]",notification.getClass().getSimpleName(),notification.getMemberToken()));
                if(notification instanceof JoinedAndReadyNotificationSignal) {
                    fireTopologyChanged(((JoinedAndReadyNotificationSignal)notification).getCurrentView());
                } else if(notification instanceof PlannedShutdownSignal) {
                    fireTopologyChanged(((PlannedShutdownSignal)notification).getCurrentView());
                } else if(notification instanceof FailureNotificationSignal) {
                    fireTopologyChanged(((FailureNotificationSignal) notification).getCurrentView());
                }
            }
        };

        //gms.addActionFactory(new FailureRecoveryActionFactoryImpl(gmsCallback));
        gms.addActionFactory(new JoinNotificationActionFactoryImpl(gmsCallback));
        gms.addActionFactory(new JoinedAndReadyNotificationActionFactoryImpl(gmsCallback));
        gms.addActionFactory(new GroupLeadershipNotificationActionFactoryImpl(gmsCallback));
        gms.addActionFactory(new FailureSuspectedActionFactoryImpl(gmsCallback));
        gms.addActionFactory(new FailureNotificationActionFactoryImpl(gmsCallback));
        gms.addActionFactory(new PlannedShutdownActionFactoryImpl(gmsCallback));

        return gms;
    }

    private void fireTopologyChanged(AliveAndReadyView currentView) {
        List<String> coreMembers = gms.getGroupHandle().getCurrentCoreMembers();
        //AliveAndReadyView coreView = gms.getGroupHandle().getCurrentAliveAndReadyCoreView();
        logger.info("fireTopologyChanged members in view: "+coreMembers.toString());
        try {
            // @todo: keep track of the previous schedule and cancel it if possible
            //scheduledExecutorService.schedule(new RebalancingRunnable(convert(coreMembers)),5, TimeUnit.SECONDS);
            scheduledExecutorService.submit(new RebalancingRunnable(convert(coreMembers)));
        } catch (IOException e) {
            //@todo: do a clean shutdown here
            logger.error("Exception on fireTopologyChanged -> Aborting",e);
        }
    }

    private List<PhysicalNode> convert(List<String> coreMembers) throws IOException {
        List<PhysicalNode> clusterNodes = new LinkedList<>();
        for (String member : coreMembers) {
            if(nodeId.equals(member)) {
                clusterNodes.add(new PhysicalNodeImpl(nodeId,nodeAddress,true));
            } else {
                InetAddress address = InetAddress.getByName((String) gms.getMemberDetails(member).get("address"));
                clusterNodes.add(new PhysicalNodeImpl(member,address,false));
            }
        }
        return clusterNodes;
    }

    public void join() {
        // send the cluster we're ready
        reportReady();

        try {
            waitLatch.await();
        } catch (InterruptedException e) {
            //
        }
    }

    public void reportReady() {
        gms.reportJoinedAndReadyState();
    }

    public void reportPlannedShutdown() {
        gms.shutdown(GMSConstants.shutdownType.INSTANCE_SHUTDOWN);
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

    private final class RebalancingRunnable implements Runnable {
        private final List<PhysicalNode> clusterNodes;

        private RebalancingRunnable(List<PhysicalNode> clusterNodes) {
            this.clusterNodes = clusterNodes;
        }

        @Override
        public void run() {
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
        }
    }


}
