package org.elasticsoftware.elasticactors.tracing;

import brave.ErrorParser;
import brave.Tracing;
import brave.context.slf4j.MDCScopeDecorator;
import brave.propagation.B3Propagation;
import brave.propagation.CurrentTraceContext.ScopeDecorator;
import brave.propagation.ThreadLocalCurrentTraceContext;
import brave.sampler.Sampler;
import org.springframework.context.annotation.Bean;
import zipkin2.Span;
import zipkin2.reporter.Reporter;

import java.util.Map;
import java.util.Map.Entry;
import java.util.StringJoiner;

public final class TestTracingConfiguration {

    private static final ErrorParser PRINTER = new ErrorParser() {
        @Override protected void error(Throwable error, Object customizer) {
            System.out.println(String.format(
                    "Thread [%s]: Error reported, class=%s, message=%s",
                    Thread.currentThread().getName(),
                    error.getClass().getName(),
                    error.getMessage()));
        }
    };
    private static final Reporter<Span> SPAN_REPORTER = span ->
            System.out.println(String.format(
                    "Thread [%s]: Span reported, name=%s, spanId=%s, parentId=%s, traceId=%s, tags=%s",
                    Thread.currentThread().getName(),
                    span.name(),
                    span.id(),
                    span.parentId(),
                    span.traceId(),
                    toString(span.tags())));
    private static final ScopeDecorator DECORATOR = (currentSpan, scope) -> {
        System.out.println(String.format(
                "Thread [%s]: Decorated scope, spanId=%s, parentId=%s, traceId=%s",
                Thread.currentThread().getName(),
                currentSpan.spanIdString(),
                currentSpan.parentIdString(),
                currentSpan.traceIdString()));
        return scope;
    };

    @Bean
    public Tracing tracing() {
        return Tracing.newBuilder()
                .currentTraceContext(ThreadLocalCurrentTraceContext.newBuilder()
                        .addScopeDecorator(MDCScopeDecorator.create())
                        .addScopeDecorator(DECORATOR)
                        .build())
                .sampler(Sampler.ALWAYS_SAMPLE)
                .spanReporter(SPAN_REPORTER)
                .errorParser(PRINTER)
                .propagationFactory(B3Propagation.FACTORY)
                .build();
    }

    private static String toString(Map<?, ?> map) {
        if (map != null) {
            StringJoiner joiner = new StringJoiner(",", "{", "}");
            for (Entry<?, ?> entry : map.entrySet()) {
                joiner.add("{\"" + entry.getKey() + "\":\"" + entry.getValue() + "\"}");
            }
            return joiner.toString();
        }
        return "null";
    }

}
