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

package org.elasticsoftware.elasticactors.kafka.testapp;

import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.ActorSystem;
import org.elasticsoftware.elasticactors.cluster.ClusterService;
import org.elasticsoftware.elasticactors.kafka.testapp.actors.VirtualCashAccountActor;
import org.elasticsoftware.elasticactors.kafka.testapp.configuration.ContainerConfiguration;
import org.elasticsoftware.elasticactors.kafka.testapp.messages.BalanceQuery;
import org.elasticsoftware.elasticactors.kafka.testapp.messages.CreditAccountEvent;
import org.elasticsoftware.elasticactors.kafka.testapp.messages.DebitAccountEvent;
import org.elasticsoftware.elasticactors.kafka.testapp.messages.ScheduleDebitCommand;
import org.elasticsoftware.elasticactors.kafka.testapp.messages.TransferCommand;
import org.elasticsoftware.elasticactors.kafka.testapp.messages.VirtualCashAccountAdapter;
import org.elasticsoftware.elasticactors.kafka.testapp.state.VirtualCashAccountState;
import org.elasticsoftware.elasticactors.messaging.UUIDTools;
import org.elasticsoftware.elasticactors.spring.AnnotationConfigApplicationContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.concurrent.CountDownLatch;

public class KafkaTestApplication {
    private static final Logger logger = LoggerFactory.getLogger(KafkaTestApplication.class);

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

            // wait a bit to let the ActorSystem initialize
            logger.info("Waiting 5 seconds for the ActorSystem cluster to stabilize...");
            Thread.sleep(5000);
            logger.info("Starting test");

            ActorSystem actorSystem = context.getBean(ActorSystem.class);
            //String firstAccountId = UUID.randomUUID().toString();
            String firstAccountId = "1eb6b4b0-20c1-4861-89be-1446aacefb03";
            ActorRef firstAccountRef = actorSystem.actorOf("accounts/"+ firstAccountId, VirtualCashAccountActor.class,
                    new VirtualCashAccountState(firstAccountId, "EUR", 2));
            //String secondAccountId = UUID.randomUUID().toString();
            String secondAccountId = "341519da-c84f-4762-8091-63efd087656c";
            ActorRef secondAccountRef = actorSystem.actorOf("accounts/"+ secondAccountId, VirtualCashAccountActor.class,
                    new VirtualCashAccountState(secondAccountId, "EUR", 2));

            // put some money on both accounts
            firstAccountRef.tell(new CreditAccountEvent(new BigDecimal("100.00")), null);
            secondAccountRef.tell(new CreditAccountEvent(new BigDecimal("100.00")), null);

            // check the balance
            VirtualCashAccountAdapter accountAdapter = firstAccountRef.ask(new BalanceQuery(), VirtualCashAccountAdapter.class).toCompletableFuture().get();

            logger.info(firstAccountId+ " account balance: "+accountAdapter.getBalance());

            accountAdapter = secondAccountRef.ask(new BalanceQuery(), VirtualCashAccountAdapter.class).toCompletableFuture().get();
            logger.info(secondAccountId+" account balance: "+accountAdapter.getBalance());

            secondAccountRef.tell(new ScheduleDebitCommand(new DebitAccountEvent(new BigDecimal("100.00"))), null);

            accountAdapter = firstAccountRef.ask(
                new TransferCommand(new BigDecimal("50.00"), "EUR",
                    firstAccountId, secondAccountId, UUIDTools.createRandomUUIDString()
                ),
                VirtualCashAccountAdapter.class
            ).toCompletableFuture().get();

            logger.info(firstAccountId+" account balance: "+accountAdapter.getBalance());

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
