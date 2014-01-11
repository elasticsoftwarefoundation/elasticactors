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

package org.elasticsoftware.elasticactors.cassandra;

import org.apache.cassandra.service.CassandraDaemon;
import org.apache.cassandra.service.IEndpointLifecycleSubscriber;
import org.apache.cassandra.service.StorageService;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.util.ReflectionUtils;

import java.io.IOException;
import java.lang.reflect.Field;

public class CassandraDaemonWrapper {

    public static void main(String... args) throws Exception {
        setup();
        // Initialize Cassandra
        CassandraDaemon.main(args);

    }

    private static void setup() throws Exception {
        // make sure log4j is initialized
        //CassandraDaemon.initLog4j();
        Class.forName("org.apache.cassandra.service.CassandraDaemon");
        // Initialize Spring
        ClassPathXmlApplicationContext applicationContext = new ClassPathXmlApplicationContext("cluster-beans.xml", "netty-beans.xml");
        // register the Lifecycle Listener
        StorageService.instance.register(applicationContext.getBean(IEndpointLifecycleSubscriber.class));
    }

    /**
     * Initialize the Cassandra Daemon based on the given <a
     * href="http://commons.apache.org/daemon/jsvc.html">Commons
     * Daemon</a>-specific arguments. To clarify, this is a hook for JSVC.
     *
     * @param arguments the arguments passed in from JSVC
     * @throws java.io.IOException
     */
    public void init(String[] arguments) throws IOException {
        try {
            setup();
        } catch (Exception e) {
            throw new IOException(e);
        }
        // now we need an instance to access the internal state
        CassandraDaemon instance = getCassandraDaemonInstance();
        instance.init(arguments);
    }

    /**
     * Start the Cassandra Daemon, assuming that it has already been
     * initialized via {@link #init(String[])}
     * <p/>
     * Hook for JSVC
     */
    public void start() {
        getCassandraDaemonInstance().start();
    }

    /**
     * Stop the daemon, ideally in an idempotent manner.
     * <p/>
     * Hook for JSVC
     */
    public void stop() {
        getCassandraDaemonInstance().stop();
    }

    /**
     * Clean up all resources obtained during the lifetime of the daemon. This
     * is a hook for JSVC.
     */
    public void destroy() {
        getCassandraDaemonInstance().stop();
    }

    protected final CassandraDaemon getCassandraDaemonInstance() {
        try {
            Field instanceField = CassandraDaemon.class.getDeclaredField("instance");
            ReflectionUtils.makeAccessible(instanceField);
            return (CassandraDaemon) ReflectionUtils.getField(instanceField, null);
        } catch (Exception e) {
            // @todo: log the error
            return null;
        }
    }


}