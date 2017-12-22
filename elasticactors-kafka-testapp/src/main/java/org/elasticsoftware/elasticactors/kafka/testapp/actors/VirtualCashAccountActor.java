package org.elasticsoftware.elasticactors.kafka.testapp.actors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsoftware.elasticactors.Actor;
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.MessageHandler;
import org.elasticsoftware.elasticactors.MethodActor;
import org.elasticsoftware.elasticactors.base.serialization.JacksonSerializationFramework;
import org.elasticsoftware.elasticactors.cluster.ActorSystemEvent;
import org.elasticsoftware.elasticactors.kafka.testapp.messages.*;
import org.elasticsoftware.elasticactors.kafka.testapp.state.VirtualCashAccountState;
import org.elasticsoftware.elasticactors.state.PersistenceConfig;

@Actor(serializationFramework = JacksonSerializationFramework.class, stateClass = VirtualCashAccountState.class)
@PersistenceConfig(excluded = {BalanceQuery.class}, persistOn = {})
public class VirtualCashAccountActor extends MethodActor {
    private static final Logger logger  = LogManager.getLogger(VirtualCashAccountActor.class);

    @Override
    public void postCreate(ActorRef creator) throws Exception {
        VirtualCashAccountState state = getState(VirtualCashAccountState.class);
        logger.info(state.getId()+".postCreate");
        getSystem().getEventListenerRegistry().register(getSelf(), ActorSystemEvent.ACTOR_SHARD_INITIALIZED, new ActivateAccountCommand());
    }

    @Override
    public void postActivate(String previousVersion) throws Exception {
        VirtualCashAccountState state = getState(VirtualCashAccountState.class);
        logger.info(state.getId()+".postActivate");
    }

    /**
     * increase the account balance
     *
     * @param event
     * @param state
     */
    @MessageHandler
    public void handle(CreditAccountEvent event, VirtualCashAccountState state) {
        logger.info(String.format("Account %s credited with %s %s", state.getId(), event.getAmount().toPlainString(), state.getCurrency()));
        state.setBalance(state.getBalance().add(event.getAmount()));
    }

    /**
     * decrease the account balance
     *
     * @param event
     * @param state
     */
    @MessageHandler
    public void handle(DebitAccountEvent event, VirtualCashAccountState state) {
        System.out.println(String.format("Account %s debited with %s %s", state.getId(), event.getAmount().toPlainString(), state.getCurrency()));
        state.setBalance(state.getBalance().subtract(event.getAmount()));
    }

    @MessageHandler
    public void handle(BalanceQuery query, VirtualCashAccountState state, ActorRef replyRef) {
        logger.info(String.format("Account %s has balance of %s %s", state.getId(), state.getBalance().toPlainString(), state.getCurrency()));
        replyRef.tell(new VirtualCashAccountAdapter(state.getBalance(), state.getCurrency()));
    }

    @MessageHandler
    public void handle(ActivateAccountCommand command, VirtualCashAccountState state) {
        logger.info(state.getId()+" received ActivateAccountCommand");
    }
}
