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

package org.elasticsoftware.elasticactors.activemq;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.elasticsoftware.elasticactors.MessageDeliveryException;
import org.elasticsoftware.elasticactors.messaging.InternalMessage;
import org.elasticsoftware.elasticactors.messaging.MessageQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.activemq.artemis.api.core.Message.HDR_DUPLICATE_DETECTION_ID;
import static org.elasticsoftware.elasticactors.messaging.UUIDTools.toByteArray;

/**
 * @author Joost van de Wijgerd
 */
public final class RemoteMessageQueue implements MessageQueue {

    private final static Logger logger = LoggerFactory.getLogger(RemoteMessageQueue.class);

    private final String queueName;
    private final String routingKey;
    private final ClientSession clientSession;
    private final ClientProducer producer;
    private final AtomicBoolean recovering = new AtomicBoolean(false);

    RemoteMessageQueue(String queueName, String routingKey, ClientSession clientSession, ClientProducer clientProducer) throws ActiveMQException {
        this.queueName = queueName;
        this.routingKey = routingKey;
        this.clientSession = clientSession;
        this.producer = clientProducer;
    }

    @Override
    public boolean offer(InternalMessage message) {
        // see if we are recovering first
        if(this.recovering.get()) {
            throw new MessageDeliveryException("MessagingService is recovering",true);
        }

        ClientMessage clientMessage = clientSession.createMessage(message.isDurable());
        clientMessage.getBodyBuffer().writeBytes(message.toByteArray());
        clientMessage.putStringProperty("routingKey", routingKey);
        // duplicate detection
        clientMessage.putBytesProperty(HDR_DUPLICATE_DETECTION_ID, toByteArray(message.getId()));
        // set timeout if needed
        if(message.getTimeout() >= 0) {
            clientMessage.setExpiration(System.currentTimeMillis() + message.getTimeout());
        }
        try {
            producer.send(clientMessage);
            return true;
        } catch (ActiveMQException e) {
            throw new MessageDeliveryException("IOException while publishing message",e,false);
        } /*catch(SomeRecoverableException e) { @todo: figure out which exceptions are recoverable
            this.recovering.set(true);
            throw new MessageDeliveryException("MessagingService is recovering",true);
        } */

    }

    @Override
    public boolean add(InternalMessage message) {
        return offer(message);
    }

    @Override
    public InternalMessage poll() {
        return null;
    }

    @Override
    public String getName() {
        return queueName;
    }

    @Override
    public void initialize() throws Exception {
        logger.info("Starting remote message queue [{}->{}]", routingKey, queueName);
        clientSession.start();
    }

    @Override
    public void destroy() {
        try {
            logger.info("Stopping remote message queue [{}->{}]", routingKey, queueName);
            producer.close();
            clientSession.close();
        } catch(ActiveMQException e) {
            // @todo: add logging
        }
    }
}
