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

package org.elasticsoftware.elasticactors.test.configuration;

import org.elasticsoftware.elasticactors.messaging.MessageQueueFactory;
import org.elasticsoftware.elasticactors.test.messaging.TestMessagingService;
import org.elasticsoftware.elasticactors.test.messaging.UnsupportedMessageQueueFactory;
import org.elasticsoftware.elasticactors.util.concurrent.ThreadBoundExecutor;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;

/**
 * @author Joost van de Wijgerd
 */
public class MessagingConfiguration {

    @Bean(name = {"messagingService", "localMessageQueueFactory"})
    public TestMessagingService getMessagingService(
        @Qualifier("queueExecutor") ThreadBoundExecutor queueExecutor)
    {
        return new TestMessagingService(queueExecutor);
    }

    @Bean(name = {"remoteMessageQueueFactory"})
    public MessageQueueFactory getRemoteMessageQueueFactory() {
        return new UnsupportedMessageQueueFactory();
    }
}
