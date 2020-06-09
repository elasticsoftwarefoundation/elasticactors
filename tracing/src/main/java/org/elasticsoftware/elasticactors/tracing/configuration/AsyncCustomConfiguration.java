package org.elasticsoftware.elasticactors.tracing.configuration;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.AsyncConfigurer;

@Configuration(proxyBeanMethods = false)
public class AsyncCustomConfiguration implements BeanPostProcessor {

    @Override
    public Object postProcessBeforeInitialization(Object bean, String beanName)
            throws BeansException {
        return bean;
    }

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName)
            throws BeansException {
        if (bean instanceof AsyncConfigurer
                && !(bean instanceof LazyTraceAsyncCustomizer)) {
            AsyncConfigurer configurer = (AsyncConfigurer) bean;
            return new LazyTraceAsyncCustomizer(configurer);
        }
        return bean;
    }

}