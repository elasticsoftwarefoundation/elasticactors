package org.elasticsoftware.elasticactors.caching.service;

import org.elasticsoftware.elasticactors.util.ClassLoadingHelper;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * An implementation of {@link ClassLoadingHelper} that cached loaded classes.
 */
public final class CachingClassLoadingHelper extends ClassLoadingHelper {

    private final Map<String, Class<?>> classCache;

    public CachingClassLoadingHelper() {
        classCache = new ConcurrentHashMap<>();
    }

    private final static class WrappedException extends RuntimeException {

        public WrappedException(ClassNotFoundException cause) {
            super(cause);
        }

        @Override
        public ClassNotFoundException getCause() {
            return (ClassNotFoundException) super.getCause();
        }
    }

    /**
     * Loads a class using {@link Class#forName} and cached the result of this invocation.
     * Subsequent invocations will use the cached classes, if they were found.
     */
    @Nonnull
    @Override
    public Class<?> forName(@Nonnull String className) throws ClassNotFoundException {
        try {
            return classCache.computeIfAbsent(className, name -> {
                try {
                    return Class.forName(name);
                } catch (ClassNotFoundException e) {
                    throw new WrappedException(e);
                }
            });
        } catch (WrappedException e) {
            throw e.getCause();
        }
    }

    @Override
    public boolean isCachingEnabled() {
        return true;
    }
}
