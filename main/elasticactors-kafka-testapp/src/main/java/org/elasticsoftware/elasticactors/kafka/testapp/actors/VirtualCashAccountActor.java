/*
 * Copyright 2013 - 2025 The Original Authors
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

package org.elasticsoftware.elasticactors.kafka.testapp.actors;

import org.elasticsoftware.elasticactors.*;
import org.elasticsoftware.elasticactors.base.serialization.JacksonSerializationFramework;
import org.elasticsoftware.elasticactors.cluster.ActorSystemEvent;
import org.elasticsoftware.elasticactors.kafka.testapp.messages.*;
import org.elasticsoftware.elasticactors.kafka.testapp.state.VirtualCashAccountState;
import org.elasticsoftware.elasticactors.state.PersistenceConfig;

import java.util.concurrent.TimeUnit;

@Actor(serializationFramework = JacksonSerializationFramework.class, stateClass = VirtualCashAccountState.class)
@PersistenceConfig(excluded = {BalanceQuery.class, ScheduleDebitCommand.class}, persistOn = {})
public class VirtualCashAccountActor extends MethodActor {

    @Override
    public void postCreate(ActorRef creator) throws Exception {
        VirtualCashAccountState state = getState(VirtualCashAccountState.class);
        logger.info("{}.postCreate", state.getId());
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
        logger.info("Account {} credited with {} {}", state.getId(), event.getAmount().toPlainString(), state.getCurrency());
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
        logger.info("Account {} debited with {} {}", state.getId(), event.getAmount().toPlainString(), state.getCurrency());
        state.setBalance(state.getBalance().subtract(event.getAmount()));
    }

    @MessageHandler
    public void handle(BalanceQuery query, VirtualCashAccountState state, ActorRef replyRef) {
        logger.info("Account {} has balance of {} {}", state.getId(), state.getBalance().toPlainString(), state.getCurrency());
        replyRef.tell(new VirtualCashAccountAdapter(state.getBalance(), state.getCurrency()));
    }

    @MessageHandler
    public void handle(ActivateAccountCommand command, VirtualCashAccountState state) {
        logger.info("{} received ActivateAccountCommand", state.getId());
    }

    @MessageHandler
    public void handle(ScheduleDebitCommand command, ActorSystem actorSystem) {
        logger.info("Scheduling message of type {} to run in 10 seconds", command.getMessage().getClass().getSimpleName());
        actorSystem.getScheduler()
            .scheduleOnce(command.getMessage(), getSelf(), 10, TimeUnit.SECONDS);
    }

    @MessageHandler
    public void handle(TransferCommand command, VirtualCashAccountState state, ActorSystem actorSystem, ActorRef replyRef) {
        logger.info("Transfer {} {} from {} to {}", command.getAmount().toPlainString(),
                command.getCurrency(), command.getFromAccount(), command.getToAccount());
        // so some sanity checking
        if(command.getFromAccount().equals(state.getId())) {
            // we need to debit this account (if we have enough money)
            handle(new DebitAccountEvent(command.getAmount()), state);
            // and the tell the other side to do a credit
            actorSystem.actorFor("accounts/"+command.getToAccount()).tell(command);
            // send our balance back
            replyRef.tell(new VirtualCashAccountAdapter(state.getBalance(), state.getCurrency()));
        } else {
            // we need to credit this account
            handle(new CreditAccountEvent(command.getAmount()), state);
        }
    }
}
