package org.elasticsoftware.elasticactors.test.configuration;

import org.elasticsoftware.elasticactors.cluster.scheduler.ScheduledMessageRepository;
import org.elasticsoftware.elasticactors.state.PersistentActorRepository;
import org.elasticsoftware.elasticactors.test.cluster.scheduler.NoopScheduledMessageRepository;
import org.elasticsoftware.elasticactors.test.state.NoopPersistentActorRepository;
import org.springframework.context.annotation.Bean;

/**
 * @author Joost van de Wijgerd
 */
public class BackplaneConfiguration {
    @Bean(name = {"persistentActorRepository"})
    public PersistentActorRepository getPersistentActorRepository() {
        return new NoopPersistentActorRepository();
    }

    @Bean(name = {"scheduledMessageRepository"})
    public ScheduledMessageRepository getScheduledMessageRepository() {
        return new NoopScheduledMessageRepository();
    }
}
