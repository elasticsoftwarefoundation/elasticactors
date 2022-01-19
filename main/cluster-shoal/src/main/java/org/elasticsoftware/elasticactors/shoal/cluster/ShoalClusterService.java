/*
 *   Copyright 2013 - 2022 The Original Authors
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

package org.elasticsoftware.elasticactors.shoal.cluster;

import com.sun.enterprise.ee.cms.core.AliveAndReadyView;
import com.sun.enterprise.ee.cms.core.CallBack;
import com.sun.enterprise.ee.cms.core.FailureNotificationSignal;
import com.sun.enterprise.ee.cms.core.GMSConstants;
import com.sun.enterprise.ee.cms.core.GMSException;
import com.sun.enterprise.ee.cms.core.GMSFactory;
import com.sun.enterprise.ee.cms.core.GroupLeadershipNotificationSignal;
import com.sun.enterprise.ee.cms.core.GroupManagementService;
import com.sun.enterprise.ee.cms.core.JoinedAndReadyNotificationSignal;
import com.sun.enterprise.ee.cms.core.MessageSignal;
import com.sun.enterprise.ee.cms.core.PlannedShutdownSignal;
import com.sun.enterprise.ee.cms.core.ServiceProviderConfigurationKeys;
import com.sun.enterprise.ee.cms.impl.client.FailureNotificationActionFactoryImpl;
import com.sun.enterprise.ee.cms.impl.client.FailureSuspectedActionFactoryImpl;
import com.sun.enterprise.ee.cms.impl.client.GroupLeadershipNotificationActionFactoryImpl;
import com.sun.enterprise.ee.cms.impl.client.JoinNotificationActionFactoryImpl;
import com.sun.enterprise.ee.cms.impl.client.JoinedAndReadyNotificationActionFactoryImpl;
import com.sun.enterprise.ee.cms.impl.client.MessageActionFactoryImpl;
import com.sun.enterprise.ee.cms.impl.client.PlannedShutdownActionFactoryImpl;
import com.sun.enterprise.mgmt.transport.grizzly.GrizzlyConfigConstants;
import org.elasticsoftware.elasticactors.PhysicalNode;
import org.elasticsoftware.elasticactors.cluster.ClusterEventListener;
import org.elasticsoftware.elasticactors.cluster.ClusterMessageHandler;
import org.elasticsoftware.elasticactors.cluster.ClusterService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Joost van de Wijgerd
 */
public final class ShoalClusterService implements ClusterService {
    private static final Logger logger = LoggerFactory.getLogger(ShoalClusterService.class);
    private static final String COMPONENT_NAME = "ElasticActors";
    private final String clusterName;
    private final String nodeId;
    private final InetAddress nodeAddress;
    private final Integer nodePort;
    private final String discoveryNodes;
    private final AtomicBoolean startupLeadershipSignal = new AtomicBoolean(true);
    private final Queue<ClusterEventListener> eventListeners = new ConcurrentLinkedQueue<>();
    private GroupManagementService gms;
    private ClusterMessageHandler clusterMessageHandler;

    public ShoalClusterService(String clusterName, String nodeId, InetAddress nodeAddress,Integer nodePort,@Nullable String discoveryNodes) {
        this.clusterName = clusterName;
        this.nodeId = nodeId;
        this.nodeAddress = nodeAddress;
        this.nodePort = nodePort;
        this.discoveryNodes = discoveryNodes;
    }

    @PostConstruct
    public void init() throws GMSException {
        gms = initializeGMS(nodeId, clusterName, nodeAddress.getHostAddress());
        // @todo: when we do this here the system might initialize to soon, now doing it on the reportReady
        // gms.join();
        // gms.updateMemberDetails(nodeId,"address",nodeAddress.getHostAddress());
    }

    @PreDestroy
    public void destroy() {
        // should be okay to call this twice (will be handled by the library)
        reportPlannedShutdown();
    }

    @Override
    public void reportReady() throws Exception {
        // @todo: the first two lines used to happen in the init
        gms.join();
        gms.updateMemberDetails(nodeId,"address",nodeAddress.getHostAddress());
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

    @Override
    public void removeEventListener(ClusterEventListener eventListener) {
        this.eventListeners.remove(eventListener);
    }

    @Override
    public void sendMessage(final String memberToken, final byte[] message) throws Exception {
        gms.getGroupHandle().sendMessage(memberToken,COMPONENT_NAME,message);
    }

    @Override
    public void setClusterMessageHandler(ClusterMessageHandler clusterMessageHandler) {
        this.clusterMessageHandler = clusterMessageHandler;
        // @todo: need to fire current master event to new message handler
    }

    private GroupManagementService initializeGMS(String serverName,String groupName, String interfaceName) throws GMSException {
        Properties props = new Properties();

        props.setProperty(ServiceProviderConfigurationKeys.MULTICASTADDRESS.toString(),"229.9.1.1");
        props.setProperty(GrizzlyConfigConstants.BIND_INTERFACE_NAME.toString(),interfaceName);
        props.setProperty(GrizzlyConfigConstants.TCPSTARTPORT.toString(),nodePort.toString());
        props.setProperty(GrizzlyConfigConstants.TCPENDPORT.toString(),nodePort.toString());
        if(discoveryNodes != null) {
            props.setProperty(GrizzlyConfigConstants.DISCOVERY_URI_LIST.toString(),discoveryNodes);
        }

        GroupManagementService gms =
                (GroupManagementService) GMSFactory.startGMSModule(serverName, groupName, GroupManagementService.MemberType.CORE, props);


        final CallBack gmsCallback = notification -> {
            logger.info("Got signal [{}] from member [{}]", notification.getClass().getSimpleName(), notification.getMemberToken());
            if(notification instanceof JoinedAndReadyNotificationSignal) {
                fireTopologyChanged(((JoinedAndReadyNotificationSignal)notification).getCurrentView());
            } else if(notification instanceof PlannedShutdownSignal) {
                fireTopologyChanged(((PlannedShutdownSignal)notification).getCurrentView());
            } else if(notification instanceof FailureNotificationSignal) {
                fireTopologyChanged(((FailureNotificationSignal) notification).getCurrentView());
            } else if(notification instanceof GroupLeadershipNotificationSignal) {
                fireLeadershipChanged((GroupLeadershipNotificationSignal)notification);
            }
        };

        final CallBack messagingCallback = notification -> {
            if(notification instanceof MessageSignal) {
                if(clusterMessageHandler != null) {
                    MessageSignal messageSignal = (MessageSignal) notification;
                    try {
                        clusterMessageHandler.handleMessage(messageSignal.getMessage(),messageSignal.getMemberToken());
                    } catch (Exception e) {
                        logger.error("Exception while handling MessageSignal from member {}, signal bytes (HEX): -", messageSignal.getMemberToken(),e);
                    }
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
        gms.addActionFactory(new MessageActionFactoryImpl(messagingCallback),COMPONENT_NAME);


        return gms;
    }

    private void fireTopologyChanged(AliveAndReadyView currentView) {
        List<String> coreMembers = gms.getGroupHandle().getCurrentCoreMembers();
        logger.info("fireTopologyChanged members in view: {}", coreMembers);
        for (ClusterEventListener eventListener : eventListeners) {
            try {
                eventListener.onTopologyChanged(convert(coreMembers));
            } catch (Exception e) {
                //@todo: do a clean shutdown here
                logger.error("Exception on fireTopologyChanged -> Aborting",e);
            }
        }
    }

    private synchronized void fireLeadershipChanged(GroupLeadershipNotificationSignal signal) {
        // we seem to be getting a false signal initially, however sometimes we seem to be getting the false signal
        // (which is always for ourselves) before the real signal
        if(!startupLeadershipSignal.compareAndSet(true,false)) {
            logger.info("fireLeadershipChanged member: "+signal.getMemberToken());
            for (ClusterEventListener eventListener : eventListeners) {
                try {
                    eventListener.onMasterElected(convert(Collections.singletonList(signal.getMemberToken())).get(0));
                } catch (Exception e) {
                    logger.error("Exception on fireLeadershipChanged",e);
                }
            }
        } else if(!signal.getMemberToken().equals(nodeId)) {
            // now the second signal is false, ensure the next signal will be ignored
            startupLeadershipSignal.set(true);
            // fire the event
            logger.info("fireLeadershipChanged member: "+signal.getMemberToken());
            for (ClusterEventListener eventListener : eventListeners) {
                try {
                    eventListener.onMasterElected(convert(Collections.singletonList(signal.getMemberToken())).get(0));
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
                clusterNodes.add(new PhysicalNode(nodeId, nodeAddress, true));
            } else {
                InetAddress address = InetAddress.getByName((String) gms.getMemberDetails(member).get("address"));
                clusterNodes.add(new PhysicalNode(member, address, false));
            }
        }
        return clusterNodes;
    }
}
