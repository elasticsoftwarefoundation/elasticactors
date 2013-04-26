/*
 * Copyright 2013 Joost van de Wijgerd
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

package org.elasticsoftwarefoundation.elasticactors.http;

import com.google.common.base.Charsets;
import com.ning.http.client.*;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.elasterix.elasticactors.ActorRef;
import org.elasterix.elasticactors.ActorSystem;
import org.elasterix.elasticactors.test.TestActorSystem;
import org.elasticsoftwarefoundation.elasticactors.http.actors.EventStreamer;
import org.elasticsoftwarefoundation.elasticactors.http.actors.User;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.assertEquals;

/**
 * @author Joost van de Wijgerd
 */
public class HttpActorSystemTest {
    @BeforeMethod
    public void setUp() {
        BasicConfigurator.resetConfiguration();
        BasicConfigurator.configure();
    }

    @Test(enabled = true)
    public void testInContainer() throws Exception {
        ActorSystem httpSystem = TestActorSystem.create(new HttpActorSystem());
        ActorSystem testSystem = TestActorSystem.create(new HttpTestActorSystem());

        // create a couple of users
        ActorRef user1Ref = testSystem.actorOf("users/1", User.class);
        ActorRef user2Ref = testSystem.actorOf("users/2",User.class);
        ActorRef user3Ref = testSystem.actorOf("users/3",User.class);

        AsyncHttpClient testClient = new AsyncHttpClient();
        for (int i = 1; i < 4; i++) {
            ListenableFuture<Response> responseFuture = testClient.prepareGet(String.format("http://localhost:8080/users/%d",i)).execute();
            Response response = responseFuture.get();

            assertEquals(response.getContentType(),"text/plain");
            assertEquals(response.getResponseBody("UTF-8"),"HelloWorld");

        }

        // remove users
        testSystem.stop(user1Ref);
        testSystem.stop(user2Ref);
        testSystem.stop(user3Ref);

        // do it again an see if we get 404
        ListenableFuture<Response> responseFuture = testClient.prepareGet("http://localhost:8080/users/1").execute();
        Response response = responseFuture.get();

        assertEquals(response.getStatusCode(),404);
    }

    @Test(enabled = false)
    public void testEventStreaming() throws Exception {
        ActorSystem httpSystem = TestActorSystem.create(new HttpActorSystem());
        ActorSystem testSystem = TestActorSystem.create(new HttpTestActorSystem());

        // create a stream
        ActorRef steamer = testSystem.actorOf("events/testing",EventStreamer.class);

        AsyncHttpClient testClient = new AsyncHttpClient();
        final CountDownLatch waitLatch = new CountDownLatch(1);
        testClient.prepareGet("http://localhost:8080/events/testing").execute(new ServerSentEventsHandler(waitLatch));
        waitLatch.await(1, TimeUnit.MINUTES);
    }

    private static final class ServerSentEventsHandler implements AsyncHandler<Object> {
        private static final Logger logger = Logger.getLogger(ServerSentEventsHandler.class);
        private final CountDownLatch waitLatch;

        private ServerSentEventsHandler(CountDownLatch waitLatch) {
            this.waitLatch = waitLatch;
        }

        @Override
        public void onThrowable(Throwable t) {
            logger.error(t);
        }

        @Override
        public STATE onBodyPartReceived(HttpResponseBodyPart bodyPart) throws Exception {
            logger.info(new String(bodyPart.getBodyPartBytes(),Charsets.UTF_8));
            return STATE.CONTINUE;
        }

        @Override
        public STATE onStatusReceived(HttpResponseStatus responseStatus) throws Exception {
            logger.info(responseStatus.getStatusCode());
            return STATE.CONTINUE;
        }

        @Override
        public STATE onHeadersReceived(HttpResponseHeaders headers) throws Exception {
            return STATE.CONTINUE;
        }

        @Override
        public Object onCompleted() throws Exception {
            logger.info("onCompleted");
            return null;  //To change body of implemented methods use File | Settings | File Templates.
        }
    }
}
