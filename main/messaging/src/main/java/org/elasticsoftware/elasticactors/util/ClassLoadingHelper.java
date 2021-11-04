package org.elasticsoftware.elasticactors.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.Iterator;
import java.util.Optional;
import java.util.ServiceLoader;

/**
 * A helper for loading classes.
 *
 * It is advised to only use this in code that is going to be classed a lot, since some
 * implementations might cache the resulting class data.
 */
public abstract class ClassLoadingHelper {

    private final static Logger logger = LoggerFactory.getLogger(ClassLoadingHelper.class);

    /**
     * Loads a class using {@link Class#forName(String)}.
     * Implementations are free to use caching and other features in order to improve performance.
     */
    @Nonnull
    public abstract Class<?> forName(@Nonnull String className) throws ClassNotFoundException;

    public abstract boolean isCachingEnabled();

    public static ClassLoadingHelper getClassHelper() {
        return ClassLoadingHelperHolder.INSTANCE;
    }

    /*
     * Initialization-on-deman holder pattern (lazy-loaded singleton)
     * See: https://en.wikipedia.org/wiki/Initialization-on-demand_holder_idiom
     */
    private static final class ClassLoadingHelperHolder {

        private static final ClassLoadingHelper INSTANCE = loadService();

        private static ClassLoadingHelper loadService() {
            try {
                return Optional.of(ServiceLoader.load(ClassLoadingHelper.class))
                    .map(ServiceLoader::iterator)
                    .filter(Iterator::hasNext)
                    .map(ClassLoadingHelperHolder::loadFirst)
                    .orElseGet(() -> {
                        logger.warn(
                            "No implementations of ClassLoadingHelper were found. "
                                + "Using the simple, non-caching implementation.");
                        return new SimpleClassLoadingHelper();
                    });
            } catch (Exception e) {
                logger.error(
                    "Exception thrown while loading ClassLoadingHelper implementation. "
                        + "Using the simple, non-caching implementation.", e);
                return new SimpleClassLoadingHelper();
            }
        }

        private static ClassLoadingHelper loadFirst(Iterator<ClassLoadingHelper> iter) {
            ClassLoadingHelper service = iter.next();
            logger.info(
                "Loaded ClassLoadingHelper implementation [{}]. Caching enabled: {}",
                service.getClass().getName(),
                service.isCachingEnabled()
            );
            return service;
        }
    }

    /**
     * Default implementation without any actual caching
     */
    private static final class SimpleClassLoadingHelper extends ClassLoadingHelper {

        /**
         * Just loads a class using {@link Class#forName(String)}.
         */
        @Nonnull
        @Override
        public Class<?> forName(@Nonnull String className) throws ClassNotFoundException {
            return Class.forName(className);
        }

        @Override
        public boolean isCachingEnabled() {
            return false;
        }
    }
}
