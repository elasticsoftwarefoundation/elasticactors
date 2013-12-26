/*
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 *
 * Copyright (c) 1997-2011 Oracle and/or its affiliates. All rights reserved.
 *
 * The contents of this file are subject to the terms of either the GNU
 * General Public License Version 2 only ("GPL") or the Common Development
 * and Distribution License("CDDL") (collectively, the "License").  You
 * may not use this file except in compliance with the License.  You can
 * obtain a copy of the License at
 * https://glassfish.dev.java.net/public/CDDL+GPL_1_1.html
 * or packager/legal/LICENSE.txt.  See the License for the specific
 * language governing permissions and limitations under the License.
 *
 * When distributing the software, include this License Header Notice in each
 * file and include the License file at packager/legal/LICENSE.txt.
 *
 * GPL Classpath Exception:
 * Oracle designates this particular file as subject to the "Classpath"
 * exception as provided by Oracle in the GPL Version 2 section of the License
 * file that accompanied this code.
 *
 * Modifications:
 * If applicable, add the following below the License Header, with the fields
 * enclosed by brackets [] replaced by your own identifying information:
 * "Portions Copyright [year] [name of copyright owner]"
 *
 * Contributor(s):
 * If you wish your version of this file to be governed by only the CDDL or
 * only the GPL Version 2, indicate your decision by adding "[Contributor]
 * elects to include this software in this distribution under the [CDDL or GPL
 * Version 2] license."  If you don't indicate a single choice of license, a
 * recipient has the option to distribute your version of this file under
 * either the CDDL, the GPL Version 2 or to extend the choice of license to
 * its licensees as provided above.  However, if you add GPL Version 2 code
 * and therefore, elected the GPL Version 2 license, then the option applies
 * only if the new code is made subject to such option by the copyright
 * holder.
 */

package org.elasticsoftware.elasticactors.shoal;

import com.sun.enterprise.ee.cms.core.*;
import com.sun.enterprise.ee.cms.impl.base.Utility;
import com.sun.enterprise.ee.cms.impl.client.FailureNotificationActionFactoryImpl;
import com.sun.enterprise.ee.cms.impl.client.JoinNotificationActionFactoryImpl;
import com.sun.enterprise.ee.cms.impl.client.JoinedAndReadyNotificationActionFactoryImpl;
import com.sun.enterprise.ee.cms.impl.client.PlannedShutdownActionFactoryImpl;
import com.sun.enterprise.ee.cms.impl.common.GroupManagementServiceImpl;
import com.sun.enterprise.ee.cms.logging.GMSLogDomain;
import com.sun.enterprise.ee.cms.spi.MemberStates;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This is an example used to demonstrate the application layer that controls the
 * lifecycle of the GMS module. It also provides an example of the actions taken
 * in response to a recover call from the GMS layer.
 *
 * @author Shreedhar Ganapathy"
 * @version $Revision$
 */
public class ApplicationServer implements Runnable, CallBack {
    private static final Logger logger = GMSLogDomain.getLogger(GMSLogDomain.GMS_LOGGER);

    public GroupManagementService gms = null;
    private GMSClientService gcs1;
    private GMSClientService gcs2;
    private GMSAdminAgent gaa;
    private String serverName;
    private String groupName;
    final private GroupManagementService.MemberType memberType;
    private volatile boolean stopped = false;
    private long lifeTime=0;
    private static final String TXLOGLOCATION = "TX_LOG_DIR";

    public ApplicationServer(final String serverName, final String groupName,
                             final GroupManagementService.MemberType memberType,
                             final Properties props) {
        this.serverName = serverName;
        this.groupName = groupName;
        this.memberType = memberType;
        GMSFactory.setGMSEnabledState(groupName, Boolean.TRUE);
        gms = (GroupManagementService) GMSFactory.startGMSModule(serverName, groupName, memberType, props);
        this.lifeTime = Long.parseLong(System.getProperty("LIFEINMILLIS", "30000"));
        initClientServices(Boolean.valueOf(System.getProperty("MESSAGING_MODE", "true")));
    }

    private void initClientServices(boolean sendMessages) {
        gcs1 = new GMSClientService("EJBContainer", serverName, sendMessages);
        gcs2 = new GMSClientService("TransactionService", serverName, false);

        gaa = new GMSAdminAgent(gms, groupName, serverName, lifeTime);
    }

    /*    private static void setupLogHandler() {
          final ConsoleHandler consoleHandler = new ConsoleHandler();
          try {
              consoleHandler.setLevel(Level.ALL);
              consoleHandler.setFormatter(new NiceLogFormatter());
          } catch( SecurityException e ) {
              new ErrorManager().error(
                   "Exception caught in setting up ConsoleHandler ",
                   e, ErrorManager.GENERIC_FAILURE );
          }
          logger.addHandler(consoleHandler);
          logger.setUseParentHandlers(false);
          final String level = System.getProperty("LOG_LEVEL","FINEST");
          logger.setLevel(Level.parse(level));
      }
    */
    public void run() {
        startGMS();
        addMemberDetails();
        startClientServices();
        try {
            // simulate other start up overhead before sending joined and ready.
            // without this sleep, not all instances will see each others joined and ready.
            Thread.sleep(3000);
        } catch (InterruptedException ie) {}
        logger.log(Level.FINE,"reporting joined and ready state...");
        gms.reportJoinedAndReadyState();
        
        logger.log(Level.INFO, "Waiting for timeout or group shutdown...");
        gaa.waitTillNotified();

        stopClientServices();
        stopGMS();
        System.exit(0);
    }

    private void addMemberDetails() {
        final Map<Object, Object> details = new Hashtable<Object, Object>();
        final ArrayList<ArrayList> ar1 = new ArrayList<ArrayList>();
        final ArrayList<String> ar2 = new ArrayList<String>();
        final int port = 3700;
        final int port1 = 3800;
        try {
            ar2.add(InetAddress.getLocalHost() + ":" + port);
            ar2.add(InetAddress.getLocalHost() + ":" + port1);
        }
        catch (UnknownHostException e) {
            logger.log(Level.WARNING, e.getLocalizedMessage());
        }
        ar1.add(ar2);
        details.put(GMSClientService.IIOP_MEMBER_DETAILS_KEY, ar1);
        try {
            ((GroupManagementServiceImpl) gms).setMemberDetails(serverName, details);
            ((GroupManagementServiceImpl) gms).updateMemberDetails(serverName, TXLOGLOCATION, serverName + "/txlog.data");
        }
        catch (GMSException e) {
            logger.log(Level.WARNING, e.getLocalizedMessage());
        }
    }

    public void startClientServices() {
        logger.log(Level.FINE, "ApplicationServer: Starting GMSClient");
        gcs1.start();
        gcs2.start();
    }

    public void startGMS() {
        logger.log(Level.FINE, "ApplicationServer: Starting GMS service");
        gms.addActionFactory(new JoinedAndReadyNotificationActionFactoryImpl(this));
        gms.addActionFactory(new JoinNotificationActionFactoryImpl(this));
        gms.addActionFactory(new PlannedShutdownActionFactoryImpl(this));
        gms.addActionFactory(new FailureNotificationActionFactoryImpl(this));
        try {
            gms.join();
            logger.info("joined group " + gms.getGroupName()  + " groupLeader: " + gms.getGroupHandle().getGroupLeader());
            
        } catch (GMSException ge) {
            logger.log(Level.SEVERE, "failed to join gms group " + gms.getGroupName(), ge);
            throw new IllegalStateException("failed to join gms group" + gms.getGroupName());
        }
    }

    public void stopGMS() {
        logger.log(Level.FINE, "ApplicationServer: Stopping GMS service");
        gms.shutdown(gaa.getShutdownType());
    }

    private void stopClientServices() {
        logger.log(Level.FINE, "ApplicationServer: Stopping GMSClient");
        gcs1.stopClient();
        gcs2.stopClient();
        stopped = true;
    }

    public void sendMessage(final String message) {
        final GroupHandle gh = gms.getGroupHandle();
        try {
            gh.sendMessage(null, message.getBytes());
        } catch (GMSException e) {
            logger.log(Level.INFO, e.getLocalizedMessage());
        }
    }

    public void processNotification(Signal notification) {
        if (notification.getMemberToken().equals("admincli")) {
            return;
        }
        MemberStates[] states;
        logger.fine("received a notification " + notification.getClass().getName());

        String rejoin = "";
        if (notification instanceof JoinedAndReadyNotificationSignal) {
            JoinedAndReadyNotificationSignal readySignal = (JoinedAndReadyNotificationSignal)notification;
            RejoinSubevent rjse = readySignal.getRejoinSubevent();
            if (rjse != null)  {
                rejoin = " Rejoining: missed FAILURE detection of instance that joined group at " + new Date(rjse.getGroupJoinTime()) + " instance has already rejoined group";

            }
        }
        logger.info("processing notification " + notification.getClass().getName() + " for group " +
                notification.getGroupName() + " memberName=" + notification.getMemberToken() + rejoin);
        if (notification instanceof FailureNotificationSignal ||
                notification instanceof PlannedShutdownSignal ||
                notification instanceof JoinedAndReadyNotificationSignal) {
            AliveAndReadyView previous = gms.getGroupHandle().getPreviousAliveAndReadyCoreView();
            AliveAndReadyView current = gms.getGroupHandle().getCurrentAliveAndReadyCoreView();
            logger.info("ASprocessNotification(" + notification.getClass().getSimpleName() + " for member:" +
                         notification.getMemberToken() +  " previous AliveAndReadyView: " + previous);
            logger.info("ASprocessNotification(" + notification.getClass().getSimpleName() + " for member:" +
                         notification.getMemberToken() +  "current AliveAndReadyView: " + current);
        }
        if (notification instanceof JoinNotificationSignal) {
            JoinNotificationSignal joinSignal = (JoinNotificationSignal)notification;
            if (joinSignal.getRejoinSubevent() != null) {
                logger.info("ApplicationServer: join for member:" + joinSignal.getMemberToken() + "rejoin subevent: " + joinSignal.getRejoinSubevent() );
            }
        }
        if (notification instanceof JoinedAndReadyNotificationSignal) {
            // getMemberState constraint check for member being added.
            MemberStates state = gms.getGroupHandle().getMemberState(notification.getMemberToken());
//            if (state != MemberStates.STARTING &&
//                state != MemberStates.READY &&
//                state != MemberStates.ALIVEANDREADY &&
//                state != MemberStates.ALIVE) {
//                logger.warning("incorrect memberstate inside of JoinedAndReadyNotification signal processing " +
//                            " expected: STARTING, READY or ALIVEANDREADY, actual value: " + state);
//            } else {
//                logger.info("getMemberState(" + notification.getMemberToken() + ")=" + state);
//            }

            // getMemberState constraint check for all core members.
            JoinedAndReadyNotificationSignal readySignal = (JoinedAndReadyNotificationSignal)notification;
            List<String> currentCoreMembers = readySignal.getCurrentCoreMembers();
            states = new MemberStates[currentCoreMembers.size()];
            int i = 0;
            for (String instanceName : currentCoreMembers ) {
                states[i] = gms.getGroupHandle().getMemberState(instanceName, 6000, 3000);
                switch (states[i]) {
                    case STARTING:
                    case ALIVE:
                    case READY:
                    case ALIVEANDREADY:
                        logger.fine("JoinedAndReadyNotificationSignal for member: " + notification.getMemberToken() + " group:" +
                        notification.getGroupName() + " expected member state for core member: " + instanceName + " memberState:" + states[i]);
                        break;
                    case UNKNOWN:
                        states[i] = gms.getGroupHandle().getMemberState(instanceName, 6000, 3000);
                        if (states[i] != MemberStates.UNKNOWN) {
                            break;
                        }

                    case INDOUBT:
                    case PEERSTOPPING:
                    case CLUSTERSTOPPING:
                    case DEAD:
                    default:
                      logger.warning("JoinedAndReadyNotificationSignal for member: " + notification.getMemberToken() + " group:" +
                        notification.getGroupName() + " unexpected member state for core member: " + instanceName + " memberState:" + states[i]);
                }
            }
            if (logger.isLoggable(Level.FINE)) {
                StringBuffer sb = new StringBuffer(110);
                sb.append("JoinedAndReadyNotificationSignal for member: " + notification.getMemberToken() + " CurrentCoreMembersSize: " +
                          currentCoreMembers.size() + " CurrentCoreMembers:");
                int ii = 0;
                for (String member : currentCoreMembers) {
                    sb.append(member + ":" + states[ii].toString() + ",");
                }
                logger.fine(sb.substring(0, sb.length() - 1));
            }
        }

    }
    
    // simulate CLB polling getMemberState
    public class CLB implements Runnable {
        boolean getMemberState;
        long threshold;
        long timeout;
        
        public CLB(boolean getMemberState, long threshold, long timeout) {
            this.getMemberState = getMemberState;
            this.threshold = threshold;
            this.timeout = timeout;
        }
        
        private void getAllMemberStates() {
            long startTime = System.currentTimeMillis();
            List<String> members = gms.getGroupHandle().getCurrentCoreMembers();
            logger.info("Enter getAllMemberStates currentMembers=" + members.size() + " threshold(ms)=" + threshold +
                          " timeout(ms)=" + timeout);
            for (String member : members) {
                MemberStates state = gms.getGroupHandle().getMemberState(member, threshold, timeout);
                logger.info("getMemberState member=" + member + " state=" + state + 
                        " threshold=" + threshold + " timeout=" + timeout);
            }
            logger.info("exit getAllMemberStates()  elapsed time=" + (System.currentTimeMillis() - startTime) +
                    " ms " + "currentMembers#=" + members.size());
        }
        
        public void run() {
            while (getMemberState && !stopped) {
                getAllMemberStates();
                try { 
                    Thread.sleep(500);
                } catch (InterruptedException ie) {}
            }
        }
    }

    public static void main(final String[] args) {
        CLB clb = null;
        if (args.length > 0 && "--usage".equals(args[1])) {
            logger.log(Level.INFO, new StringBuffer().append("USAGE: java -DMEMBERTYPE <CORE|SPECTATOR|WATCHDOG>")
                    .append(" -DINSTANCEID=<instanceid>")
                    .append(" -DCLUSTERNAME=<clustername")
                    .append(" -DLIFEINMILLIS= <length of time for this demo")
                    .append(" -DMAX_MISSED_HEARTBEATS=<indoubt-after-missing-this-many>")
                    .append(" -DHEARTBEAT_FREQUENCY=<in-milliseconds>")
                    .append(" -DMESSAGING_MODE=[true|false] ApplicationServer")
                    .append(" -DGETMEMBERSTATE=[true]")
                    .append(" -DGETMEMBERSTATE_THRESHOLD=[xxxx] ms")
                    .append(" -DGETMEMBERSTATE_TIMEOUT=[xxx] ms")
                    .append(" -DKILLINSTANCE=<anotherinstanceid>")
                    .toString());
        }
        Utility.setLogger(logger);
        Utility.setupLogHandler();
        String monitorLogLevel = System.getProperty("MONITOR_LOG_LEVEL");
        System.out.println("MONITOR_LOG_LEVEL = " + monitorLogLevel);
        if (monitorLogLevel != null) {
            Level monitorLevel = Level.INFO;
            try {
                monitorLevel = Level.parse(monitorLogLevel);
                GMSLogDomain.getMonitorLogger().setLevel(monitorLevel);

            } catch (Throwable t) {
                logger.warning("invalid value for MONITOR_LOG_LEVEL: [" + monitorLogLevel+ "]");
            }
            System.out.println("ShoalLogger.monitor level=" + GMSLogDomain.getMonitorLogger().getLevel());
        }

        final ApplicationServer applicationServer;
        final String MEMBERTYPE_STRING = System.getProperty("MEMBERTYPE", "CORE").toUpperCase();
        final GroupManagementService.MemberType memberType = GroupManagementService.MemberType.valueOf(MEMBERTYPE_STRING);
        
        Properties configProps = new Properties();
        configProps.put(ServiceProviderConfigurationKeys.MONITORING.toString(), "5"); //seconds

        String discovery_uri_list = System.getProperty("DISCOVERY_URI_LIST");
        if (discovery_uri_list != null) {
            // non-multicast mode, seed with Master URI. Only work on single machine test case, not implemented for distributed test execution now.
            configProps.put(ServiceProviderConfigurationKeys.DISCOVERY_URI_LIST.toString(), "http://127.0.0.1:9090");  // non-multicast mode
        } else {
            //multicast mode
            configProps.put(ServiceProviderConfigurationKeys.MULTICASTADDRESS.toString(),
                            System.getProperty("MULTICASTADDRESS", "229.9.1.1"));
            configProps.put(ServiceProviderConfigurationKeys.MULTICASTPORT.toString(),
                            Integer.parseInt(System.getProperty("MULTICASTPORT", "2299")));
            logger.info("multicastaddress:" + configProps.get("MULTICASTADDRESS")  + " multicastport:" + configProps.get("MULTICASTPORT"));
            logger.fine("Is initial host="+System.getProperty("IS_INITIAL_HOST"));

             configProps.put(ServiceProviderConfigurationKeys.IS_BOOTSTRAPPING_NODE.toString(),
                System.getProperty("IS_INITIAL_HOST", "false"));
        }
        //if(System.getProperty("INITIAL_HOST_LIST") != null){
        //    configProps.put(ServiceProviderConfigurationKeys.DISCOVERY_URI_LIST.toString(),
        //        System.getProperty("INITIAL_HOST_LIST"));
        //}
        configProps.put(ServiceProviderConfigurationKeys.FAILURE_DETECTION_RETRIES.toString(),
                        System.getProperty("MAX_MISSED_HEARTBEATS", "3"));
        configProps.put(ServiceProviderConfigurationKeys.FAILURE_DETECTION_TIMEOUT.toString(),
                        System.getProperty("HEARTBEAT_FREQUENCY", "2000"));
        //Uncomment this to receive loop back messages
        //configProps.put(ServiceProviderConfigurationKeys.LOOPBACK.toString(), "true");
        final String bindInterfaceAddress = System.getProperty("BIND_INTERFACE_ADDRESS");
        if(bindInterfaceAddress != null){
            configProps.put(ServiceProviderConfigurationKeys.BIND_INTERFACE_ADDRESS.toString(),bindInterfaceAddress );
        }

        applicationServer = new ApplicationServer(System.getProperty("INSTANCEID"), System.getProperty("CLUSTERNAME"), memberType, configProps);
        if ("true".equals(System.getProperty("GETMEMBERSTATE"))) {
            boolean getMemberState = true;
            String threshold = System.getProperty("GETMEMBERSTATE_THRESHOLD","3000");
            long getMemberStateThreshold = Long.parseLong(threshold);
            long getMemberStateTimeout = Long.parseLong(System.getProperty("GETMEMBERSTATE_TIMEOUT", "3000"));
            logger.fine("getMemberState=true threshold=" + getMemberStateThreshold + 
                    " timeout=" + getMemberStateTimeout);
            clb = applicationServer.new CLB(getMemberState, getMemberStateThreshold, getMemberStateTimeout);
        }
        final Thread appServThread = new Thread(applicationServer, "ApplicationServer");
        appServThread.start();
        try {
            if (clb != null && ! applicationServer.isWatchdog()){
                final Thread clbThread = new Thread(clb, "CLB");
                clbThread.start();
            }
            // developer level manual WATCHDOG test.
            // Start each of the following items in a different terminal window.
            // Fix permissions for shell scripts: chmod +x rungmsdemo.sh killmembers.sh
            // 1. ./rungmsdemo.sh server cluster1 SPECTATOR 600000 FINE &> server.log 
            // 2. ./rungmsdemo.sh instance1 cluster1 CORE 600000 FINE
            // 3. ./rungmsdemo.sh instance10 cluster1 CORE 600000 FINE
            // 4. ./rungmsdemo.sh nodeagent cluster1 WATCHDOG 600000 FINE
            //
            // If WATCHDOG, then test reporting failure to cluster. 
            // kill instance10 15 seconds after starting nodeagent WATCHDOG.  
            // Broadcast failure and check server.log for
            // immediate FAILURE detection, not FAILURE detected by GMS heartbeat.
            // grep server.log for WATCHDOG to see watchdog notification time compared to FAILURE report.
            if (applicationServer.isWatchdog()) {
                try {
                    Thread.sleep(15000);
                    final String TOBEKILLED_MEMBER="instance10";

                    GroupHandle gh = applicationServer.gms.getGroupHandle();
                    Runtime.getRuntime().exec("./killmembers.sh " + TOBEKILLED_MEMBER);
                    logger.info("killed member " + TOBEKILLED_MEMBER);
                    gh.announceWatchdogObservedFailure(TOBEKILLED_MEMBER);
                    logger.info("Killed instance10 and WATCHDOG notify group " + gh.toString() + " that instance10 has failed.");
                } catch (Exception e) {
                    logger.log(Level.SEVERE, "Unexpected exception while starting server instance for WATCHDOG to kill and report failed",
                            e);
                }
            }
            appServThread.join();
        } catch (InterruptedException e) {
            logger.log(Level.SEVERE, e.getLocalizedMessage());
        }
        logger.info("ApplicationServer finished");
    }

    boolean isWatchdog() {
        return memberType == GroupManagementService.MemberType.WATCHDOG;
    }
}
