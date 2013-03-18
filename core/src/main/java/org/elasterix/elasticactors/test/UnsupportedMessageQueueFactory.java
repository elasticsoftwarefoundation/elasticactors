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

package org.elasterix.elasticactors.test;

import org.elasterix.elasticactors.messaging.MessageHandler;
import org.elasterix.elasticactors.messaging.MessageQueue;
import org.elasterix.elasticactors.messaging.MessageQueueFactory;

/**
 * @author Joost van de Wijgerd
 */
public class UnsupportedMessageQueueFactory implements MessageQueueFactory {
    @Override
    public MessageQueue create(String name, MessageHandler messageHandler) throws Exception {
        throw new UnsupportedOperationException("Remote Queues not supported in test mode");
    }
}
