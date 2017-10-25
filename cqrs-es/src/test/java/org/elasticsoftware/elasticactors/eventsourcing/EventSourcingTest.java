package org.elasticsoftware.elasticactors.eventsourcing;

import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.ActorSystem;
import org.elasticsoftware.elasticactors.eventsourcing.commands.TransferCommand;
import org.elasticsoftware.elasticactors.eventsourcing.commands.TransferCompletedResponse;
import org.elasticsoftware.elasticactors.eventsourcing.queries.BalanceQuery;
import org.elasticsoftware.elasticactors.eventsourcing.queries.BalanceQueryResponse;
import org.elasticsoftware.elasticactors.test.TestActorSystem;
import org.iban4j.CountryCode;
import org.iban4j.Iban;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.util.concurrent.CountDownLatch;

public class EventSourcingTest {
    @Test
    public void testDebitAccount() throws Exception {
        TestActorSystem testActorSystem = new TestActorSystem();
        testActorSystem.initialize();

        ActorSystem actorSystem = testActorSystem.getActorSystem();

        Iban fromIban = new Iban.Builder()
                .countryCode(CountryCode.NL)
                .bankCode("EAES")
                .buildRandom();

        Iban toIban = new Iban.Builder()
                .countryCode(CountryCode.NL)
                .bankCode("EAES")
                .buildRandom();

        CountDownLatch waitLatch = new CountDownLatch(1);

        ActorRef fromAccount = actorSystem.actorOf("accounts/"+fromIban.toString(), VirtualBankAccountActor.class, new VirtualBankAccountState(fromIban.toString(), "EUR", new BigDecimal("1000.00")));
        ActorRef toAccount = actorSystem.actorOf("accounts/"+toIban.toString(), VirtualBankAccountActor.class, new VirtualBankAccountState(toIban.toString(), "EUR", new BigDecimal("0.00")));

        fromAccount.ask(new TransferCommand(fromIban.toString(), toIban.toString(), new BigDecimal("100.00")), TransferCompletedResponse.class)
                .toCompletableFuture()
                .whenComplete((transferOkResponse, throwable) -> {
                    if(transferOkResponse != null) {
                        System.out.println("response ok");
                    } else {
                        throwable.printStackTrace();
                    }
                    waitLatch.countDown();
                }

                );

        waitLatch.await();

        System.out.println(fromIban.toFormattedString() + " : " + fromAccount.ask(new BalanceQuery(), BalanceQueryResponse.class)
                .toCompletableFuture().get().getVirtualBankAccount().getBalance().toPlainString());

        System.out.println(toIban.toFormattedString() + " : " + toAccount.ask(new BalanceQuery(), BalanceQueryResponse.class)
                .toCompletableFuture().get().getVirtualBankAccount().getBalance().toPlainString());


        testActorSystem.destroy();
    }
}
