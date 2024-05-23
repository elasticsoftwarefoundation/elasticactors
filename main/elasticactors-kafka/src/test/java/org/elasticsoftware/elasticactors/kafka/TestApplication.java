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

package org.elasticsoftware.elasticactors.kafka;

import org.elasticsoftware.elasticactors.cluster.ClusterService;
import org.elasticsoftware.elasticactors.kafka.configuration.ContainerConfiguration;
import org.elasticsoftware.elasticactors.spring.AnnotationConfigApplicationContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

public class TestApplication {
    private static final Logger logger = LoggerFactory.getLogger(TestApplication.class);

    public static void main(String[] args) {
        logger.info("Starting container");
        try {
            // initialize all the beans
            AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(ContainerConfiguration.class);
            // create the http handler from the context
            //HttpHandler handler = WebHttpHandlerBuilder.applicationContext(context).build();

            // start the cluster
            ClusterService clusterService = context.getBean(ClusterService.class);
            try {
                clusterService.reportReady();
            } catch (Exception e) {
                throw new RuntimeException("Exception in ClusterService.reportReady()", e);
            }

            /*

            ServletHttpHandlerAdapter servlet = new ServletHttpHandlerAdapter(handler);

            Tomcat tomcatServer = new Tomcat();
            //tomcatServer.setHostname(DEFAULT_HOST);
            tomcatServer.setPort(8080);
            Context rootContext = tomcatServer.addContext("", System.getProperty("java.io.tmpdir"));
            // need to add this for websocket support
            rootContext.addServletContainerInitializer(new WsSci(), null);
            Tomcat.addServlet(rootContext, "httpHandlerServlet", servlet);
            rootContext.addServletMappingDecoded("/", "httpHandlerServlet");
            tomcatServer.start();

            */

            final CountDownLatch waitLatch = new CountDownLatch(1);

            Runtime.getRuntime().addShutdownHook(new Thread(waitLatch::countDown));

            try {
                waitLatch.await();
            } catch (InterruptedException e) {
                // do nothing
            }
            // stop accepting http requests
            /*
            try {
                tomcatServer.stop();
            } catch(Exception e) {
                logger.error("Unexpected exception stopping embedded tomcat server");
            }
            */
            // signal to the others we are going to leave the cluster
            try {
                clusterService.reportPlannedShutdown();
            } catch (Exception e) {
                logger.error("UnexpectedException on reportPlannedShutdown()", e);
            }
            // close the context (this will shut down the shards)
            context.close();
            // sleep a little here
            Thread.sleep(10000);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
