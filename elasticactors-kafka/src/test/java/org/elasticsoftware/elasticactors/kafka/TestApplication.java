package org.elasticsoftware.elasticactors.kafka;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsoftware.elasticactors.cluster.ClusterService;
import org.elasticsoftware.elasticactors.kafka.configuration.ContainerConfiguration;
import org.elasticsoftware.elasticactors.spring.AnnotationConfigApplicationContext;
import org.springframework.http.server.reactive.HttpHandler;
import org.springframework.web.server.adapter.WebHttpHandlerBuilder;

import java.util.concurrent.CountDownLatch;

public class TestApplication {
    private static final Logger logger = LogManager.getLogger(TestApplication.class);

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
