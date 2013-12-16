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


public class GMSAdminConstants  {
    // Messages the gmsadmincli and the GMSAdminAgent can send back and forth
    public static final String STOPCLUSTER = "stop_cluster";
    public static final String STOPCLUSTERREPLY = "stop_cluster_reply";
    public static final String STOPINSTANCE = "stop_instance";
    public static final String STOPINSTANCEREPLY = "stop_instance_reply";
    public static final String KILLINSTANCE = "kill_instance";
    public static final String KILLINSTANCEREPLY = "kill_instance_reply";
    public static final String KILLALL = "kill_all";
    public static final String STOPCLUSTERRECEIVED = "stop_cluster_received";
    public static final String ISSTARTUPCOMPLETE = "is_startup_complete";
    public static final String ISSTARTUPCOMPLETEREPLY = "isstartup_complete_reply";
    public static final String STARTUPCOMPLETE = "startup_complete";
    public static final String STARTTESTING = "start_testing";
    public static final String TESTINGCOMPLETE = "testing_complete";


    // various states the GMSAdminAgent can go through
    public static final int UNASSIGNED = -1;
    public static final int RUN = 0;
    public static final int STOP = 1;
    public static final int SHUTDOWNCLUSTER = 2;
    public static final int KILL = 3;


    public static final String ADMINAGENT = "adminagent";
    public static final String ADMINCLI = "admincli";
    public static final String ADMINNAME = "server";
    public static final String INSTANCEPREFIX = "instance";
    public static final String TESTCOORDINATOR = "TestCoordinator";
    public static final String TESTEXECUTOR = "TestExceutor";




}
