package org.elasticsoftware.elasticactors.tracing.configuration;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.elasticsoftware.elasticactors.tracing.MessagingContextManager.MessagingScope;
import org.elasticsoftware.elasticactors.tracing.TraceContext;

import static org.elasticsoftware.elasticactors.tracing.MessagingContextManager.currentTraceContext;
import static org.elasticsoftware.elasticactors.tracing.MessagingContextManager.enter;

@Aspect
class TraceAsyncAspect {

    @Around("execution (@org.springframework.scheduling.annotation.Async  * *.*(..))")
    public Object traceBackgroundThread(final ProceedingJoinPoint pjp) throws Throwable {
        TraceContext traceContext = currentTraceContext();
        if (traceContext == null) {
            try (MessagingScope ignored = enter(new TraceContext())) {
                return pjp.proceed();
            }
        }
        return pjp.proceed();
    }

}
