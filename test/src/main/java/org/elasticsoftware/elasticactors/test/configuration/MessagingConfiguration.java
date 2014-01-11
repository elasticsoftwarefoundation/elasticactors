package org.elasticsoftware.elasticactors.test.configuration;

import org.elasticsoftware.elasticactors.messaging.MessageQueueFactory;
import org.elasticsoftware.elasticactors.messaging.MessagingService;
import org.elasticsoftware.elasticactors.test.messaging.TestMessagingService;
import org.elasticsoftware.elasticactors.test.messaging.UnsupportedMessageQueueFactory;
import org.elasticsoftware.elasticactors.util.concurrent.ThreadBoundExecutor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;

import javax.annotation.PostConstruct;

/**
 * @author Joost van de Wijgerd
 */
public class MessagingConfiguration {
    @Autowired @Qualifier("queueExecutor")
    private ThreadBoundExecutor<String> queueExecutor;
    private TestMessagingService messagingService;

    @PostConstruct
    public void init() {
        messagingService = new TestMessagingService(queueExecutor);
    }

    @Bean(name = {"messagingService"})
    public MessagingService getMessagingService() {
        return messagingService;
    }

    @Bean(name = {"localMessageQueueFactory"})
    public MessageQueueFactory getLocalMessageQueueFactory() {
        return messagingService;
    }

    @Bean(name = {"remoteMessageQueueFactory"})
    public MessageQueueFactory getRemoteMessageQueueFactory() {
        return new UnsupportedMessageQueueFactory();
    }
}
