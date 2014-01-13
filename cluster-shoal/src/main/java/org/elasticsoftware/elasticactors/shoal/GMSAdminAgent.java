/*
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 *
 * Copyright (c) 2010 Oracle and/or its affiliates. All rights reserved.
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
import com.sun.enterprise.ee.cms.impl.client.JoinNotificationActionFactoryImpl;
import com.sun.enterprise.ee.cms.impl.client.JoinedAndReadyNotificationActionFactoryImpl;
import com.sun.enterprise.ee.cms.impl.client.MessageActionFactoryImpl;
import com.sun.enterprise.ee.cms.impl.client.PlannedShutdownActionFactoryImpl;
import com.sun.enterprise.ee.cms.impl.common.GroupManagementServiceImpl;
import com.sun.enterprise.ee.cms.logging.NiceLogFormatter;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.ConsoleHandler;
import java.util.logging.ErrorManager;
import java.util.logging.Level;
import java.util.logging.Logger;

public class GMSAdminAgent implements CallBack {

    private static final Logger myLogger = java.util.logging.Logger.getLogger("GMSAdminAgent");
    private static final Level TESTDEFAULTLOGLEVEL = Level.INFO;
    private GroupManagementService gms;
    private String groupName;
    private String memberName;
    private long lifeTime;
    private boolean isAdmin = false;
    private static AtomicInteger NotifiedOfStateChange = new AtomicInteger(GMSAdminConstants.RUN);
    private boolean SHUTDOWNINITIATED = false;
    List<String> activeMembers = new LinkedList<String>();
    private GMSConstants.shutdownType shutdownType = GMSConstants.shutdownType.INSTANCE_SHUTDOWN;
    private static AtomicLong timeReceivedLastJoinJoinedAndReady = new AtomicLong(System.currentTimeMillis());
    private static AtomicLong diffTime = new AtomicLong(0);
    private static AtomicBoolean startupComplete = new AtomicBoolean(false);
    private static AtomicBoolean startupInitiated = new AtomicBoolean(false);
    private static ArrayList<Thread> replyThreads = new ArrayList<Thread>();

    public GMSAdminAgent(final GroupManagementService gms,
            final String groupName,
            final String memberName,
            final long lifeTime) {
        this.gms = gms;
        this.groupName = groupName;
        this.memberName = memberName.toLowerCase();
        this.lifeTime = lifeTime;

        // if the member nasme is server then it is assumed that that member is
        // the admin
        if (this.memberName.equals(GMSAdminConstants.ADMINNAME)) {
            this.isAdmin = true;
        }
        // this configures the formatting of the myLogger output
        GMSAdminAgent.setupLogHandler();

        try {
            myLogger.setLevel(Level.parse(System.getProperty("TEST_LOG_LEVEL", TESTDEFAULTLOGLEVEL.toString())));
        } catch (Exception e) {
            myLogger.setLevel(TESTDEFAULTLOGLEVEL);
        }
        myLogger.info("Test Logging using log level of:" + myLogger.getLevel());


        gms.addActionFactory(new PlannedShutdownActionFactoryImpl(this));
        gms.addActionFactory(new MessageActionFactoryImpl(this), GMSAdminConstants.ADMINAGENT);
        gms.addActionFactory(new JoinNotificationActionFactoryImpl(this));
        gms.addActionFactory(new JoinedAndReadyNotificationActionFactoryImpl(this));

        activeMembers.addAll(gms.getGroupHandle().getCurrentCoreMembers());

    }

    // returns true if shutdown was successful, false if shutdown was a result of a timeout
    public int waitTillNotified() {
        myLogger.fine("GMSAdminAgent: entering waitTillNotified");

        if (isAdmin) {

            if (lifeTime == 0) {
                // only wait on startup if we AREN"T using specific times
                if (myLogger.isLoggable(TESTDEFAULTLOGLEVEL)) {
                    myLogger.log(TESTDEFAULTLOGLEVEL, "Waiting for startup to begin");
                }
                synchronized (startupInitiated) {
                    try {
                        startupInitiated.wait(0);
                    } catch (InterruptedException ie) {
                    }
                }
                if (myLogger.isLoggable(TESTDEFAULTLOGLEVEL)) {
                    myLogger.log(TESTDEFAULTLOGLEVEL, "Startup has begun");
                }
                int previousOutstanding = 0;
                timeReceivedLastJoinJoinedAndReady.set(System.currentTimeMillis());

                while (!startupComplete.get()) {
                    // using this mechanism requires the registering of JoinNotification
                    // and JoinedAndReadyNotification
                    long currentTime = System.currentTimeMillis();
                    long diff = currentTime - timeReceivedLastJoinJoinedAndReady.get();
                    // if there are not outstanding messages and the delta time is greater than 5 seconds
                    int currentOutstanding = ((GroupManagementServiceImpl) gms).outstandingNotifications();
                    if ((currentOutstanding == 0) && (diff >= 10000)) {
                        if (myLogger.isLoggable(TESTDEFAULTLOGLEVEL)) {
                            myLogger.log(TESTDEFAULTLOGLEVEL, "Startup Complete - currentOutstanding=" + currentOutstanding + ", previousOutstanding=" + previousOutstanding + ", diff:" + diff);
                        }
                        synchronized (activeMembers) {
                            activeMembers.clear();
                            activeMembers.addAll(gms.getGroupHandle().getCurrentCoreMembers());
                            if (myLogger.isLoggable(TESTDEFAULTLOGLEVEL)) {
                                myLogger.log(TESTDEFAULTLOGLEVEL, "Initializing current list of active members:" + activeMembers.toString());
                            }
                        }
                        startupComplete.set(true);
                        synchronized (startupComplete) {
                            startupComplete.notifyAll();
                        }

                    } else if ((currentOutstanding > 0) && (currentOutstanding <= previousOutstanding) && (diff >= 120000)) {
                        // if there are outstanding messages and the time is greater than 2 minutes this is a failure
                        myLogger.severe("Waiting period exceeded 2 minutes, there appears to be an issue, we are going to assume startup is finished - currentOutstanding=" + currentOutstanding + ", previousOutstanding=" + previousOutstanding + ", diff:" + diff);
                        startupComplete.set(true);
                        synchronized (startupComplete) {
                            startupComplete.notifyAll();
                        }
                    } else {
                        if (myLogger.isLoggable(TESTDEFAULTLOGLEVEL)) {
                            myLogger.log(TESTDEFAULTLOGLEVEL, "Waiting to complete startup - currentOutstanding=" + currentOutstanding + ", previousOutstanding=" + previousOutstanding + ", diff:" + diff);
                        }
                        sleep(1);
                    }
                    previousOutstanding = currentOutstanding;
                }

            }

            /*
            try {
            if (myLogger.isLoggable(TESTDEFAULTLOGLEVEL)) {
            myLogger.log(TESTDEFAULTLOGLEVEL, "Broadcasting isstartupcomplete reply to :" + GMSAdminConstants.ADMINCLI);
            }
            gms.getGroupHandle().sendMessage(GMSAdminConstants.ADMINCLI, GMSAdminConstants.ISSTARTUPCOMPLETEREPLY.getBytes());
            if (myLogger.isLoggable(TESTDEFAULTLOGLEVEL)) {
            myLogger.log(TESTDEFAULTLOGLEVEL, "Done broadcasting isstartupcomplete reply to :" + GMSAdminConstants.ADMINCLI);
            }

            } catch (GMSException ge1) {
            myLogger.log(Level.SEVERE, "Exception occurred while broadcasting reply message: " + GMSAdminConstants.ISSTARTUPCOMPLETEREPLY + ge1, ge1);
            }

             */
            try {
                if (myLogger.isLoggable(TESTDEFAULTLOGLEVEL)) {
                    myLogger.log(TESTDEFAULTLOGLEVEL, "Broadcast startup is complete to cluster");
                }
                gms.getGroupHandle().sendMessage(GMSAdminConstants.ADMINNAME, GMSAdminConstants.STARTUPCOMPLETE.getBytes());
            } catch (GMSException ge1) {
                myLogger.log(Level.SEVERE, "Exception occurred while broadcasting message: " + GMSAdminConstants.STARTUPCOMPLETE + ge1, ge1);
            }

            if (myLogger.isLoggable(TESTDEFAULTLOGLEVEL)) {
                myLogger.log(TESTDEFAULTLOGLEVEL, "Startup Complete");
            }

            // wait until the Admin receives a shutdown message from the gmsadmincli
            synchronized (NotifiedOfStateChange) {
                try {
                    NotifiedOfStateChange.wait(lifeTime);
                } catch (InterruptedException ie) {
                }
            }
            if (NotifiedOfStateChange.get() == GMSAdminConstants.SHUTDOWNCLUSTER) {
                synchronized (activeMembers) {
                    activeMembers.clear();
                    activeMembers.addAll(gms.getGroupHandle().getCurrentCoreMembers());
                    if (myLogger.isLoggable(TESTDEFAULTLOGLEVEL)) {
                        myLogger.log(TESTDEFAULTLOGLEVEL, "Current list of active members:" + activeMembers.toString());
                    }
                }

                if (myLogger.isLoggable(TESTDEFAULTLOGLEVEL)) {
                    myLogger.log(TESTDEFAULTLOGLEVEL, "numberOfCoreMembers=" + activeMembers.size());
                }

                // tell the cluster, shutdown has started
                if (myLogger.isLoggable(TESTDEFAULTLOGLEVEL)) {
                    myLogger.log(TESTDEFAULTLOGLEVEL, "Sending GroupShutdown Initiated");
                }
                gms.announceGroupShutdown(groupName, GMSConstants.shutdownState.INITIATED);


                synchronized (activeMembers) {
                    try {
                        activeMembers.wait(15000); // wait till all activeMembers shutdown OR fifteen seconds
                    } catch (InterruptedException ie) {
                    }
                }
                List<String> dup = null;
                synchronized (activeMembers) {
                    // tell the cluster,  shutdown has completed
                    if (myLogger.isLoggable(TESTDEFAULTLOGLEVEL)) {
                        myLogger.log(TESTDEFAULTLOGLEVEL, "activeMembers=|" + activeMembers.toString() + "|");
                    }
                    if (activeMembers.size() > 0) {
                        // not all instances reported shutdown, so now individually
                        //   shutdown each one that is still active.
                        dup = new ArrayList<String>(activeMembers);
                    }
                }

                if (dup != null && dup.size() > 0) {
                    for (String member : dup) {
                        try {
                            gms.getGroupHandle().sendMessage(member, GMSAdminConstants.ADMINAGENT, GMSAdminConstants.STOPINSTANCE.getBytes());
                        } catch (MemberNotInViewException me) {
                            // member finally shutdown.  No need to report that. Definitely not a SEVERE exception.
                        } catch (GMSException e) {
                            myLogger.log(Level.SEVERE, "Exception occurred while sending stopinstance message:" + e, e);
                        }
                    }
                }
                synchronized (activeMembers) {
                    try {
                        if (activeMembers.size() > 0) {
                            activeMembers.wait(15000); // wait till all remaining activeMembers have shutdown OR fifteen seconds
                        }
                        if (activeMembers.size() > 0) {
                            myLogger.warning("Not all instances were successfully shutdown within 15 seconds: " + activeMembers.toString());
                        }
                    } catch (InterruptedException ie) {
                    }
                }

                if (myLogger.isLoggable(TESTDEFAULTLOGLEVEL)) {
                    myLogger.log(TESTDEFAULTLOGLEVEL, "Sending GroupShutdown Completed");
                }
                gms.announceGroupShutdown(groupName, GMSConstants.shutdownState.COMPLETED);
                if (myLogger.isLoggable(TESTDEFAULTLOGLEVEL)) {
                    myLogger.log(TESTDEFAULTLOGLEVEL, "GMSAdminAgent: leaving waitTillNotified");
                }
            } else if (NotifiedOfStateChange.get() == GMSAdminConstants.KILL) {

                // this is used to inject a fatal failure
                if (myLogger.isLoggable(TESTDEFAULTLOGLEVEL)) {
                    myLogger.log(TESTDEFAULTLOGLEVEL, "Killing ourselves as instructed to do so");
                }
                Runtime.getRuntime().halt(0);
            }

        } else {

            // is an instance of the cluster
            synchronized (NotifiedOfStateChange) {
                try {
                    NotifiedOfStateChange.wait(lifeTime);
                } catch (InterruptedException ie) {
                }

                if (NotifiedOfStateChange.get() == GMSAdminConstants.KILL) {

                    // this is used to inject a fatal failure
                    if (myLogger.isLoggable(TESTDEFAULTLOGLEVEL)) {
                        myLogger.log(TESTDEFAULTLOGLEVEL, "Killing ourselves as instructed to do so");
                    }
                    Runtime.getRuntime().halt(0);
                }
            }
        }
        if (myLogger.isLoggable(TESTDEFAULTLOGLEVEL)) {
            myLogger.log(TESTDEFAULTLOGLEVEL, "GMSAdminAgent: exiting waitTillNotified");
        }

        return NotifiedOfStateChange.get();

    }

    public GMSConstants.shutdownType getShutdownType() {
        return shutdownType;
    }

    public static void sleep(int i) {
        try {
            Thread.sleep(i * 1000);

        } catch (InterruptedException ex) {
        }
    }

    public synchronized void processNotification(final Signal notification) {
        final String from = notification.getMemberToken();

        if (myLogger.isLoggable(TESTDEFAULTLOGLEVEL)) {
            myLogger.log(TESTDEFAULTLOGLEVEL, "Received a NOTIFICATION from :" + from + ", " + notification);
        }

        // PLANNEDSHUTDOWN  HANDLING
        if (notification instanceof PlannedShutdownSignal) {
            // don't processess gmsadmincli shutdown messages
            if (myLogger.isLoggable(TESTDEFAULTLOGLEVEL)) {
                myLogger.log(TESTDEFAULTLOGLEVEL, "Received PlannedShutdownNotification from :" + from);
            }
            if (isAdmin) {
                if (!from.equals(GMSAdminConstants.ADMINCLI)) {
                    synchronized (activeMembers) {
                        if (myLogger.isLoggable(TESTDEFAULTLOGLEVEL)) {
                            myLogger.log(TESTDEFAULTLOGLEVEL, "Current list before remove:" + activeMembers.toString());
                        }
                        if (!activeMembers.remove(from)) {
                            myLogger.severe("Received more than one plannedshutdown from :" + from + ", current list:" + activeMembers.toString());
                        }
                        if (activeMembers.size() == 0) {
                            activeMembers.notifyAll();
                        }
                    }
                }
            } else {
                PlannedShutdownSignal psSignal = (PlannedShutdownSignal) notification;
                if (psSignal.getEventSubType().equals(GMSConstants.shutdownType.GROUP_SHUTDOWN)) {
                    shutdownType = GMSConstants.shutdownType.GROUP_SHUTDOWN;
                    NotifiedOfStateChange.set(GMSAdminConstants.SHUTDOWNCLUSTER);

                    synchronized (NotifiedOfStateChange) {
                        NotifiedOfStateChange.notifyAll();
                    }
                }
            }
        } else if (notification instanceof JoinNotificationSignal) {
            if (myLogger.isLoggable(TESTDEFAULTLOGLEVEL)) {
                myLogger.log(TESTDEFAULTLOGLEVEL, "JOIN NOTIFICATION was received from :" + from);
            }
            if (isAdmin) {

                // if we are the master and we've received an join from someone other than ourselves or admincli
                // this basically means all the core members
                if (!from.equals(GMSAdminConstants.ADMINNAME) && !from.equals(GMSAdminConstants.ADMINCLI)) {
                    timeReceivedLastJoinJoinedAndReady.set(System.currentTimeMillis());
                    if (startupInitiated.get() == false) {
                        if (myLogger.isLoggable(TESTDEFAULTLOGLEVEL)) {
                            myLogger.log(TESTDEFAULTLOGLEVEL, "STARTUP INITIATED ");
                        }
                        startupInitiated.set(true);
                        synchronized (startupInitiated) {
                            startupInitiated.notifyAll();
                        }
                    }

                    if (myLogger.isLoggable(TESTDEFAULTLOGLEVEL)) {
                        myLogger.log(TESTDEFAULTLOGLEVEL, "Resetting time received last Join: " + timeReceivedLastJoinJoinedAndReady.get());
                    }
                }
            }


        } else if (notification instanceof JoinedAndReadyNotificationSignal) {
            if (myLogger.isLoggable(TESTDEFAULTLOGLEVEL)) {
                myLogger.log(TESTDEFAULTLOGLEVEL, "JOINANDREADY NOTIFICATION was received from :" + from);
            }
            if (isAdmin) {
                // if we are the master and we've received an join from someone other than ourselves or admincli
                // this basically means all the core members
                if (!from.equals(GMSAdminConstants.ADMINNAME) && !from.equals(GMSAdminConstants.ADMINCLI)) {
                    timeReceivedLastJoinJoinedAndReady.set(System.currentTimeMillis());
                    if (myLogger.isLoggable(TESTDEFAULTLOGLEVEL)) {
                        myLogger.log(TESTDEFAULTLOGLEVEL, "Resetting time received last JoinedAndReady: " + timeReceivedLastJoinJoinedAndReady.get());
                    }
                }
            }
            // }


            // MESSAGE HANDLING
        } else if (notification instanceof MessageSignal) {
            MessageSignal messageSignal = (MessageSignal) notification;
            String msgString = new String(messageSignal.getMessage());
            if (myLogger.isLoggable(TESTDEFAULTLOGLEVEL)) {
                myLogger.log(TESTDEFAULTLOGLEVEL, "Message received from [" + from + "] was:" + msgString);
            }
            if (msgString.equals(GMSAdminConstants.STOPCLUSTER)) {
                // only allow admin to stop cluster
                if (isAdmin) {
                    if (myLogger.isLoggable(TESTDEFAULTLOGLEVEL)) {
                        myLogger.log(TESTDEFAULTLOGLEVEL, "Received stopcluster from :" + from);
                    }
                    try {
                        if (myLogger.isLoggable(TESTDEFAULTLOGLEVEL)) {
                            myLogger.log(TESTDEFAULTLOGLEVEL, "Sending stop cluster reply to :" + from);
                        }
                        gms.getGroupHandle().sendMessage(from, GMSAdminConstants.ADMINCLI, GMSAdminConstants.STOPCLUSTERREPLY.getBytes());
                        if (myLogger.isLoggable(TESTDEFAULTLOGLEVEL)) {
                            myLogger.log(TESTDEFAULTLOGLEVEL, "Done sending stopcluster reply to :" + from);
                        }

                    } catch (GMSException ge1) {
                        myLogger.log(Level.SEVERE, "Exception occurred while sending reply message: " + GMSAdminConstants.STOPCLUSTERREPLY + ge1, ge1);
                    }
                    NotifiedOfStateChange.set(GMSAdminConstants.SHUTDOWNCLUSTER);
                    synchronized (NotifiedOfStateChange) {
                        NotifiedOfStateChange.notifyAll();                    //}
                    }
                } else {
                    if (myLogger.isLoggable(TESTDEFAULTLOGLEVEL)) {
                        myLogger.log(TESTDEFAULTLOGLEVEL, "Ignoring " + GMSAdminConstants.STOPCLUSTER + " since we are not the admin");
                    }
                }
            } else if (msgString.equals(GMSAdminConstants.STOPINSTANCE)) {
                if (myLogger.isLoggable(TESTDEFAULTLOGLEVEL)) {
                    myLogger.log(TESTDEFAULTLOGLEVEL, "Received instance stop from :" + from);
                }
                try {
                    if (myLogger.isLoggable(TESTDEFAULTLOGLEVEL)) {
                        myLogger.log(TESTDEFAULTLOGLEVEL, "Sending stop instance reply to :" + from);
                    }
                    gms.getGroupHandle().sendMessage(from, GMSAdminConstants.ADMINCLI, GMSAdminConstants.STOPINSTANCEREPLY.getBytes());
                    if (myLogger.isLoggable(TESTDEFAULTLOGLEVEL)) {
                        myLogger.log(TESTDEFAULTLOGLEVEL, "Done sending stop instance reply to :" + from);
                    }

                } catch (GMSException ge1) {
                    myLogger.log(Level.SEVERE, "Exception occurred while sending reply message: " + GMSAdminConstants.STOPINSTANCEREPLY + ge1, ge1);
                }
                NotifiedOfStateChange.set(GMSAdminConstants.STOP);

                synchronized (NotifiedOfStateChange) {
                    NotifiedOfStateChange.notifyAll();                    //}
                }
            } else if (msgString.equals(GMSAdminConstants.KILLINSTANCE)) {
                if (myLogger.isLoggable(TESTDEFAULTLOGLEVEL)) {
                    myLogger.log(TESTDEFAULTLOGLEVEL, "Received kill instance from :" + from);
                }
                try {
                    if (myLogger.isLoggable(TESTDEFAULTLOGLEVEL)) {
                        myLogger.log(TESTDEFAULTLOGLEVEL, "Sending kill instance reply to :" + from);
                    }
                    gms.getGroupHandle().sendMessage(from, GMSAdminConstants.ADMINCLI, GMSAdminConstants.KILLINSTANCEREPLY.getBytes());
                    if (myLogger.isLoggable(TESTDEFAULTLOGLEVEL)) {
                        myLogger.log(TESTDEFAULTLOGLEVEL, "Done sending kill instance reply to :" + from);
                    }

                } catch (GMSException ge1) {
                    myLogger.log(Level.SEVERE, "Exception occurred while sending reply message: " + GMSAdminConstants.KILLINSTANCEREPLY + ge1, ge1);
                }
                NotifiedOfStateChange.set(GMSAdminConstants.KILL);

                synchronized (NotifiedOfStateChange) {
                    NotifiedOfStateChange.notifyAll();                    //}
                }
            } else if (msgString.equals(GMSAdminConstants.ISSTARTUPCOMPLETE)) {
                // this message is only for master
                if (isAdmin) {
                    if (myLogger.isLoggable(TESTDEFAULTLOGLEVEL)) {
                        myLogger.log(TESTDEFAULTLOGLEVEL, "Received isstartupcomplete from :" + from);
                    }
                    /*
                    if (startupComplete.get() == true) {
                    if (myLogger.isLoggable(TESTDEFAULTLOGLEVEL)) {
                    myLogger.log(TESTDEFAULTLOGLEVEL, "Startup is already complete, sending back reply message");
                    }
                    try {
                    if (myLogger.isLoggable(TESTDEFAULTLOGLEVEL)) {
                    myLogger.log(TESTDEFAULTLOGLEVEL, "Broadcasting isstartupcomplete reply to :" + from);
                    }
                    gms.getGroupHandle().sendMessage(from, GMSAdminConstants.ADMINCLI, GMSAdminConstants.ISSTARTUPCOMPLETEREPLY.getBytes());
                    if (myLogger.isLoggable(TESTDEFAULTLOGLEVEL)) {
                    myLogger.log(TESTDEFAULTLOGLEVEL, "Done broadcasting isstartupcomplete reply to :" + from);
                    }

                    } catch (GMSException ge1) {
                    myLogger.log(Level.SEVERE, "Exception occurred while sending reply message: " + GMSAdminConstants.ISSTARTUPCOMPLETEREPLY + ge1, ge1);
                    }
                    }

                     */

                    int id = replyThreads.size();
                    if (myLogger.isLoggable(TESTDEFAULTLOGLEVEL)) {
                        myLogger.log(TESTDEFAULTLOGLEVEL, "Creating reply thread :" + id);
                    }
                    replyThreads.add(new ReplyToIsStartupCompleteThread(id, from));
                    replyThreads.get(id).start();
                } else {
                    if (myLogger.isLoggable(TESTDEFAULTLOGLEVEL)) {
                        myLogger.log(TESTDEFAULTLOGLEVEL, "Ignoring message" + GMSAdminConstants.ISSTARTUPCOMPLETE + " since we are not the admin");
                    }
                }
            } else {
                if (myLogger.isLoggable(TESTDEFAULTLOGLEVEL)) {
                    myLogger.log(TESTDEFAULTLOGLEVEL, "Ignoring message:" + msgString);
                }
            }

        }


    }

    public static void setupLogHandler() {
        final ConsoleHandler consoleHandler = new ConsoleHandler();
        try {
            consoleHandler.setLevel(Level.ALL);
            consoleHandler.setFormatter(new NiceLogFormatter());
        } catch (SecurityException e) {
            new ErrorManager().error(
                    "Exception caught in setting up ConsoleHandler ",
                    e, ErrorManager.GENERIC_FAILURE);
        }
        myLogger.addHandler(consoleHandler);
        myLogger.setUseParentHandlers(false);
        //final String level = System.getProperty("LOG_LEVEL", "INFO");
        //myLogger.setLevel(Level.parse(level));
        myLogger.setLevel(TESTDEFAULTLOGLEVEL);

    }

    public class ReplyToIsStartupCompleteThread extends Thread {

        private String name = "ReplyToIsStartupCompleteThread";
        private Thread thread;
        private String from;
        private String threadName = null;

        public ReplyToIsStartupCompleteThread(int id, String from) {
            this.from = from;
            this.threadName = name + id;
        }

        @Override
        public void start() {
            thread = new Thread(this, threadName);
            if (myLogger.isLoggable(TESTDEFAULTLOGLEVEL)) {
                myLogger.log(TESTDEFAULTLOGLEVEL, "Starting " + threadName + " thread");
            }
            thread.start();
        }

        public void run() {
            if (!startupComplete.get()) {
                if (myLogger.isLoggable(TESTDEFAULTLOGLEVEL)) {
                    myLogger.log(TESTDEFAULTLOGLEVEL, "Waiting for startup to complete, before sending reply");
                }
                synchronized (startupComplete) {
                    try {
                        startupComplete.wait(0);
                    } catch (InterruptedException ie) {
                    }
                }
            } else {
                if (myLogger.isLoggable(TESTDEFAULTLOGLEVEL)) {
                    myLogger.log(TESTDEFAULTLOGLEVEL, "Startup has been completed, sending reply");
                }
            }
            try {
                if (myLogger.isLoggable(TESTDEFAULTLOGLEVEL)) {
                    myLogger.log(TESTDEFAULTLOGLEVEL, "Sending isstartupcomplete reply to :" + from);
                }
                gms.getGroupHandle().sendMessage(from, GMSAdminConstants.ADMINCLI, GMSAdminConstants.ISSTARTUPCOMPLETEREPLY.getBytes());
                if (myLogger.isLoggable(TESTDEFAULTLOGLEVEL)) {
                    myLogger.log(TESTDEFAULTLOGLEVEL, "Done sending isstartupcomplete reply to :" + from);
                }

            } catch (GMSException ge1) {
                myLogger.log(Level.SEVERE, "Exception occurred while sending reply message: " + GMSAdminConstants.ISSTARTUPCOMPLETEREPLY + " to " + from + ge1, ge1);
            }

            if (myLogger.isLoggable(TESTDEFAULTLOGLEVEL)) {
                myLogger.log(TESTDEFAULTLOGLEVEL, "Stopping " + threadName + " thread");
            }
            thread = null;
        }
    }
}
