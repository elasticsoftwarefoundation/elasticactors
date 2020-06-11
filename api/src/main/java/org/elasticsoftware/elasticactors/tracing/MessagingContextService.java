package org.elasticsoftware.elasticactors.tracing;

import org.elasticsoftware.elasticactors.ActorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.lang.reflect.Method;
import java.util.Iterator;
import java.util.Optional;
import java.util.ServiceLoader;

public final class MessagingContextService {

    private final static Logger logger = LoggerFactory.getLogger(MessagingContextService.class);

    private final static class NoopMessagingContextManager implements MessagingContextManager {

        private final static class NoopMessagingScope implements MessagingScope {

            @Nullable
            @Override
            public TraceContext getTraceContext() {
                return null;
            }

            @Override
            public boolean isClosed() {
                return false;
            }

            @Override
            public void close() {

            }
        }

        private final static MessagingScope noopMessagingScope = new NoopMessagingScope();

        @Nullable
        @Override
        public TraceContext currentTraceContext() {
            return null;
        }

        @Nullable
        @Override
        public MessageHandlingContext currentMessageHandlingContext() {
            return null;
        }

        @Nullable
        @Override
        public CreationContext currentCreationContext() {
            return null;
        }

        @Nullable
        @Override
        public CreationContext creationContextFromScope() {
            return null;
        }

        @Nullable
        @Override
        public Method currentMethodContext() {
            return null;
        }

        @Nonnull
        @Override
        public MessagingScope enter(
                @Nullable ActorContext context, @Nullable TracedMessage message) {
            return noopMessagingScope;
        }

        @Nonnull
        @Override
        public MessagingScope enter(
                @Nullable TraceContext traceContext, @Nullable CreationContext creationContext) {
            return noopMessagingScope;
        }

        @Nonnull
        @Override
        public MessagingScope enter(@Nonnull Method context) {
            return noopMessagingScope;
        }
    }

    private final static MessagingContextManager LOADED_SERVICE = loadManager();

    private static MessagingContextManager loadManager() {
        try {
            return Optional.of(ServiceLoader.load(MessagingContextManager.class))
                    .map(ServiceLoader::iterator)
                    .filter(Iterator::hasNext)
                    .map(Iterator::next)
                    .orElseGet(() -> {
                        logger.warn(
                                "No implementations of MessagingContextManager were found. "
                                        + "Falling back to no-op.");
                        return new NoopMessagingContextManager();
                    });
        } catch (Exception e) {
            logger.error(
                    "Exception thrown while loading MessagingContextManager implementation. "
                            + "Falling back to no-op.", e);
            return new NoopMessagingContextManager();
        }
    }

    @Nonnull
    public static MessagingContextManager getManager() {
        return LOADED_SERVICE;
    }

}
