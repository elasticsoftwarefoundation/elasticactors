package org.elasticsoftware.elasticactors.tracing.configuration;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.aop.interceptor.AsyncExecutionAspectSupport;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.NoUniqueBeanDefinitionException;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Role;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.annotation.AsyncConfigurerSupport;

import java.util.concurrent.Executor;

@Configuration(proxyBeanMethods = false)
public class AsyncDefaultConfiguration {

	@Bean
	public static ExecutorBeanPostProcessor executorBeanPostProcessor() {
		return new ExecutorBeanPostProcessor();
	}

	/**
	 * Wrapper for the async executor.
	 */
	@Configuration(proxyBeanMethods = false)
	@Role(BeanDefinition.ROLE_INFRASTRUCTURE)
	static class DefaultAsyncConfigurerSupport extends AsyncConfigurerSupport {

		private static final Log log = LogFactory
				.getLog(DefaultAsyncConfigurerSupport.class);

		private final BeanFactory beanFactory;

		public DefaultAsyncConfigurerSupport(BeanFactory beanFactory) {
			this.beanFactory = beanFactory;
		}

		@Override
		public Executor getAsyncExecutor() {
			Executor delegate = getDefaultExecutor();
			return new LazyTraceExecutor(delegate);
		}

		/**
		 * Retrieve or build a default executor for this advice instance. An executor
		 * returned from here will be cached for further use.
		 * <p>
		 * The default implementation searches for a unique {@link TaskExecutor} bean in
		 * the context, or for an {@link Executor} bean named "taskExecutor" otherwise. If
		 * neither of the two is resolvable, this implementation will return {@code null}.
		 * @return the default executor, or {@code null} if none available
		 */
		private Executor getDefaultExecutor() {
			try {
				// Search for TaskExecutor bean... not plain Executor since that would
				// match with ScheduledExecutorService as well, which is unusable for
				// our purposes here. TaskExecutor is more clearly designed for it.
				return this.beanFactory.getBean(TaskExecutor.class);
			}
			catch (NoUniqueBeanDefinitionException ex) {
				log.debug("Could not find unique TaskExecutor bean", ex);
				try {
					return this.beanFactory.getBean(
							AsyncExecutionAspectSupport.DEFAULT_TASK_EXECUTOR_BEAN_NAME,
							Executor.class);
				}
				catch (NoSuchBeanDefinitionException ex2) {
					if (log.isInfoEnabled()) {
						log.info(
								"More than one TaskExecutor bean found within the context, and none is named "
										+ "'taskExecutor'. Mark one of them as primary or name it 'taskExecutor' (possibly "
										+ "as an alias) in order to use it for async processing: "
										+ ex.getBeanNamesFound());
					}
				}
			}
			catch (NoSuchBeanDefinitionException ex) {
				log.debug("Could not find default TaskExecutor bean", ex);
				try {
					return this.beanFactory.getBean(
							AsyncExecutionAspectSupport.DEFAULT_TASK_EXECUTOR_BEAN_NAME,
							Executor.class);
				}
				catch (NoSuchBeanDefinitionException ex2) {
					log.info("No task executor bean found for async processing: "
							+ "no bean of type TaskExecutor and no bean named 'taskExecutor' either");
				}
				// Giving up -> either using local default executor or none at all...
			}
			// backward compatibility
			if (log.isInfoEnabled()) {
				log.info(
						"For backward compatibility, will fallback to the default, SimpleAsyncTaskExecutor implementation");
			}
			return new SimpleAsyncTaskExecutor();
		}

	}

}
