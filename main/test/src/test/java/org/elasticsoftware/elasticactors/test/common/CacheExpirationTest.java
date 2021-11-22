package org.elasticsoftware.elasticactors.test.common;

import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.ActorSystem;
import org.elasticsoftware.elasticactors.base.actors.ActorDelegate;
import org.elasticsoftware.elasticactors.base.actors.ReplyActor;
import org.elasticsoftware.elasticactors.base.state.StringState;
import org.elasticsoftware.elasticactors.test.TestActorSystem;
import org.elasticsoftware.elasticactors.test.messaging.LocalMessageQueue;
import org.elasticsoftware.elasticactors.tracing.CreationContext;
import org.elasticsoftware.elasticactors.tracing.MessagingContextManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.elasticsoftware.elasticactors.tracing.MessagingContextManager.getManager;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class CacheExpirationTest {

    private final static Logger logger = LoggerFactory.getLogger(CacheExpirationTest.class);

    private static final ThreadLocal<MessagingContextManager.MessagingScope> testScope = new ThreadLocal<>();

    @BeforeMethod
    public void addExternalCreatorData(Method method) {
        testScope.set(getManager().enter(
            GreetingTest.TEST_TRACE,
            new CreationContext(
                GreetingTest.class.getSimpleName(),
                GreetingTest.class,
                method)));
    }

    @AfterMethod
    public void removeExternalCreatorData() {
        testScope.get().close();
        assertNull(getManager().currentScope());
        testScope.remove();
    }

    @Test
    public void testWontExpire() throws Exception {
        System.setProperty("ea.nodeCache.expirationCheckPeriod", "10000");

        logger.info("Starting testGreeting");

        TestActorSystem testActorSystem = new TestActorSystem();
        testActorSystem.initialize();

        ActorSystem actorSystem = testActorSystem.getActorSystem();
        ActorRef greeter =
            actorSystem.actorOf("greeter", GreetingActor.class, new StringState("Hello World"));

        final CountDownLatch countDownLatch = new CountDownLatch(2);

        ActorRef replyActor = actorSystem.tempActorOf(ReplyActor.class, ActorDelegate.builder()
            .deleteAfterReceive(false)
            .timeout(2000L)
            .onReceive(ScheduledGreeting.class, () -> logger.info("Got Scheduled Greeting"))
            .onReceive(Greeting.class, m -> logger.info("Got Greeting from {}", m.getWho()))
            .orElse(ActorDelegate.MessageConsumer.noop())
            .postReceive(countDownLatch::countDown)
            .build());

        greeter.tell(new Greeting("Joost van de Wijgerd"), replyActor);

        assertTrue(countDownLatch.await(5, TimeUnit.SECONDS));
        assertTrue(LocalMessageQueue.getThrownExceptions().isEmpty());

        testActorSystem.destroy();
    }

    @Test
    public void testExpireOnReceive() throws Exception {
        System.setProperty("ea.nodeCache.expirationCheckPeriod", "10000");

        logger.info("Starting testGreeting");

        TestActorSystem testActorSystem = new TestActorSystem();
        testActorSystem.initialize();

        ActorSystem actorSystem = testActorSystem.getActorSystem();
        ActorRef greeter =
            actorSystem.actorOf("greeter", GreetingActor.class, new StringState("Hello World"));

        final CountDownLatch countDownLatch = new CountDownLatch(2);

        ActorRef replyActor = actorSystem.tempActorOf(ReplyActor.class, ActorDelegate.builder()
            .deleteAfterReceive(false)
            .timeout(1_500L)
            .onReceive(ScheduledGreeting.class, () -> logger.info("Got Scheduled Greeting"))
            .onReceive(Greeting.class, m -> logger.info("Got Greeting from {}", m.getWho()))
            .orElse(ActorDelegate.MessageConsumer.noop())
            .postReceive(countDownLatch::countDown)
            .build());

        Thread.sleep(1_000L);

        greeter.tell(new Greeting("Joost van de Wijgerd"), replyActor);

        assertFalse(countDownLatch.await(5, TimeUnit.SECONDS));
        assertEquals(countDownLatch.getCount(), 1);
        assertTrue(LocalMessageQueue.getThrownExceptions().isEmpty());

        testActorSystem.destroy();
    }

    @Test
    public void testExpireAsync() throws Exception {
        System.setProperty("ea.nodeCache.expirationCheckPeriod", "500");

        logger.info("Starting testGreeting");

        TestActorSystem testActorSystem = new TestActorSystem();
        testActorSystem.initialize();

        ActorSystem actorSystem = testActorSystem.getActorSystem();
        ActorRef greeter =
            actorSystem.actorOf("greeter", GreetingActor.class, new StringState("Hello World"));

        final CountDownLatch countDownLatch = new CountDownLatch(2);

        ActorRef replyActor = actorSystem.tempActorOf(ReplyActor.class, ActorDelegate.builder()
            .deleteAfterReceive(false)
            .timeout(1_000L)
            .onReceive(ScheduledGreeting.class, () -> logger.info("Got Scheduled Greeting"))
            .onReceive(Greeting.class, m -> logger.info("Got Greeting from {}", m.getWho()))
            .orElse(ActorDelegate.MessageConsumer.noop())
            .postReceive(countDownLatch::countDown)
            .build());

        Thread.sleep(2_000L);

        greeter.tell(new Greeting("Joost van de Wijgerd"), replyActor);

        assertFalse(countDownLatch.await(5, TimeUnit.SECONDS));
        assertEquals(countDownLatch.getCount(), 2);
        assertTrue(LocalMessageQueue.getThrownExceptions().isEmpty());

        testActorSystem.destroy();
    }

}
