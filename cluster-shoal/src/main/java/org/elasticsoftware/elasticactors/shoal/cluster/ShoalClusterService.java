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

package org.elasticsoftware.elasticactors.shoal.cluster;

import com.sun.enterprise.ee.cms.core.*;
import com.sun.enterprise.ee.cms.impl.client.*;
import com.sun.enterprise.mgmt.transport.grizzly.GrizzlyConfigConstants;
import org.apache.log4j.Logger;
import org.elasticsoftware.elasticactors.PhysicalNode;
import org.elasticsoftware.elasticactors.cluster.ClusterEventListener;
import org.elasticsoftware.elasticactors.cluster.ClusterService;
import org.elasticsoftware.elasticactors.cluster.PhysicalNodeImpl;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.IOException;
import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Joost van de Wijgerd
 */
public class ShoalClusterService implements ClusterService {
    private static final Logger logger = Logger.getLogger(ShoalClusterService.class);
    private final String clusterName;
    private final String nodeId;
    private final InetAddress nodeAddress;
    private final AtomicBoolean startupLeadershipSignal = new AtomicBoolean(true);
    private final Queue<ClusterEventListener> eventListeners = new ConcurrentLinkedQueue<>();
    private GroupManagementService gms;

    public ShoalClusterService(String clusterName, String nodeId, InetAddress nodeAddress) {
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
        // should be okay to call this twice (will be handled by the library)
        reportPlannedShutdown();
    }

    @Override
    public void reportReady() {
        gms.reportJoinedAndReadyState();
    }

    @Override
    public void reportPlannedShutdown() {
        gms.shutdown(GMSConstants.shutdownType.INSTANCE_SHUTDOWN);
    }

    @Override
    public void addEventListener(ClusterEventListener eventListener) {
        this.eventListeners.add(eventListener);
    }

    private GroupManagementService initializeGMS(String serverName,String groupName, String interfaceName) throws GMSException {
        Properties props = new Properties();

        props.setProperty(ServiceProviderConfigurationKeys.MULTICASTADDRESS.toString(),"229.9.1.1");
        props.setProperty(GrizzlyConfigConstants.BIND_INTERFACE_NAME.toString(),interfaceName);

        GroupManagementService gms =
                (GroupManagementService) GMSFactory.startGMSModule(serverName, groupName, GroupManagementService.MemberType.CORE, props);


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
                } else if(notification instanceof GroupLeadershipNotificationSignal) {
                    fireLeadershipChanged((GroupLeadershipNotificationSignal)notification);
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
        logger.info("fireTopologyChanged members in view: "+coreMembers.toString());
        for (ClusterEventListener eventListener : eventListeners) {
            try {
                eventListener.onTopologyChanged(convert(coreMembers));
            } catch (Exception e) {
                //@todo: do a clean shutdown here
                logger.error("Exception on fireTopologyChanged -> Aborting",e);
            }
        }
    }

    private void fireLeadershipChanged(GroupLeadershipNotificationSignal signal) {
        if(!startupLeadershipSignal.compareAndSet(true,false)) {
            logger.info("fireLeadershipChanged member: "+signal.getMemberToken());
            for (ClusterEventListener eventListener : eventListeners) {
                try {
                    eventListener.onMasterElected(convert(Arrays.asList(signal.getMemberToken())).get(0));
                } catch (Exception e) {
                    logger.error("Exception on fireLeadershipChanged",e);
                }
            }
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
}
