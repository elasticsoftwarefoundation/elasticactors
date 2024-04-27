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

package org.elasticsoftware.elasticactors.shoal;

import com.sun.enterprise.ee.cms.core.*;
import com.sun.enterprise.ee.cms.impl.client.*;
import com.sun.enterprise.mgmt.transport.grizzly.GrizzlyConfigConstants;

import java.text.MessageFormat;
import java.util.Properties;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author Joost van de Wijgerd
 */
public class GMSTest implements CallBack {
    final static Logger logger = Logger.getLogger(GMSTest.class.getSimpleName());
        final Object waitLock = new Object();

        public static void main(String[] args){
            GMSTest sgs = new GMSTest();
            try {
                sgs.runSimpleSample();
            } catch (GMSException e) {

                logger.log(Level.SEVERE, "Exception occured while joining group:"+e);
                e.printStackTrace();
            }
        }

        /**
         * Runs this sample
         * @throws GMSException
         */
        private void runSimpleSample() throws GMSException {
            logger.log(Level.INFO, "Starting SimpleShoalGMSSample....");

            final String serverName = UUID.randomUUID().toString();
            final String groupName = "Group1";

            //initialize Group Management Service
            GroupManagementService gms = initializeGMS(serverName, groupName);
            //register for Group Events
            registerForGroupEvents(gms);
            //join group
            joinGMSGroup(groupName, gms);
            try {
                //send some messages
                sendMessages(gms, serverName);
                waitForShutdown();

            } catch (InterruptedException e) {
                logger.log(Level.WARNING, e.getMessage());
            }
            //leave the group gracefully
            leaveGroupAndShutdown(serverName, gms);
        }

        private GroupManagementService initializeGMS(String serverName, String groupName) {
            logger.log(Level.INFO, "Initializing Shoal for member: "+serverName+" group:"+groupName);

            // disable multicast
            Properties props = new Properties();
            //props.setProperty(GrizzlyConfigConstants.TCPSTARTPORT.toString(),"9090");
            //props.setProperty(GrizzlyConfigConstants.TCPENDPORT.toString(),"9090");
            props.setProperty(ServiceProviderConfigurationKeys.MULTICASTADDRESS.toString(),"229.9.1.1");
            props.setProperty(GrizzlyConfigConstants.BIND_INTERFACE_NAME.toString(),"192.168.56.1");
            //props.setProperty(GrizzlyConfigConstants.DISCOVERY_URI_LIST.toString(),"tcp://192.168.56.1:9090");

            return (GroupManagementService) GMSFactory.startGMSModule(serverName,
                    groupName, GroupManagementService.MemberType.CORE, props);
        }

        private void registerForGroupEvents(GroupManagementService gms) {
            logger.log(Level.INFO, "Registering for group event notifications");
            gms.addActionFactory(new JoinNotificationActionFactoryImpl(this));
            gms.addActionFactory(new FailureSuspectedActionFactoryImpl(this));
            gms.addActionFactory(new FailureNotificationActionFactoryImpl(this));
            gms.addActionFactory(new PlannedShutdownActionFactoryImpl(this));
            gms.addActionFactory(new MessageActionFactoryImpl(this),"SimpleSampleComponent");
        }

        private void joinGMSGroup(String groupName, GroupManagementService gms) throws GMSException {
            logger.log(Level.INFO, "Joining Group "+groupName);
            gms.join();
        }

        private void sendMessages(GroupManagementService gms, String serverName) throws InterruptedException, GMSException {
            logger.log(Level.INFO, "wait 15 secs to send 10 messages");
            synchronized(waitLock){
                waitLock.wait(10000);
            }
            GroupHandle gh = gms.getGroupHandle();

            logger.log(Level.INFO, "Sending messages...");
            for(int i = 0; i<=10; i++ ){
                gh.sendMessage("SimpleSampleComponent",
                        MessageFormat.format("Message {0}from server {1}", i, serverName).getBytes());
            }
        }

        private void waitForShutdown() throws InterruptedException {
            logger.log(Level.INFO, "wait 20 secs to shutdown");
            synchronized(waitLock){
                waitLock.wait(20000);
            }
        }

        private void leaveGroupAndShutdown(String serverName, GroupManagementService gms) {
            logger.log(Level.INFO, "Shutting down instance "+serverName);
            gms.shutdown(GMSConstants.shutdownType.INSTANCE_SHUTDOWN);
            System.exit(0);
        }

        @Override
        public void processNotification(Signal signal) {
            logger.log(Level.INFO, "Received Notification of type : "+signal.getClass().getName());
            try {
                signal.acquire();
                logger.log(Level.INFO,"Source Member: "+signal.getMemberToken());
                if(signal instanceof MessageSignal){
                    logger.log(Level.INFO,"Message: "+new String(((MessageSignal)signal).getMessage()));
                }
                signal.release();
            } catch (SignalAcquireException e) {
                logger.log(Level.WARNING, "Exception occured while acquiring signal"+e);
            } catch (SignalReleaseException e) {
                logger.log(Level.WARNING, "Exception occured while releasing signal"+e);
            }

        }
}
