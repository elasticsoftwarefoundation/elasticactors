package org.elasticsoftware.elasticactors.tracing.configuration;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
public class AsyncDefaultConfiguration {

    @Bean
    public static ExecutorBeanPostProcessor executorBeanPostProcessor() {
        return new ExecutorBeanPostProcessor();
    }

}
