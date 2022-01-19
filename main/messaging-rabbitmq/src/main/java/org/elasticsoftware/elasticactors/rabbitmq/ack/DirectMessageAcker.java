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

package org.elasticsoftware.elasticactors.rabbitmq.ack;

import com.rabbitmq.client.Channel;
import org.elasticsoftware.elasticactors.rabbitmq.MessageAcker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author Joost van de Wijgerd
 */
public final class DirectMessageAcker implements MessageAcker {
    private static final Logger logger = LoggerFactory.getLogger(DirectMessageAcker.class);
    private final Channel consumerChannel;

    public DirectMessageAcker(Channel consumerChannel) {
        this.consumerChannel = consumerChannel;
    }

    @Override
    public void deliver(long deliveryTag) {
        // do nothing as we will directly ack
    }

    @Override
    public void ack(long deliveryTag) {
        try {
            consumerChannel.basicAck(deliveryTag,false);
        } catch (IOException e) {
            logger.error("Exception while acking message", e);
        }
    }

    @Override
    public void start() {
        logger.info("Using MessageAcker [{}]", getClass().getSimpleName());
        // nothing to do here
    }

    @Override
    public void stop() {
        // nothing to do here
    }
}
