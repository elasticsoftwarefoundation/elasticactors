/*
 * Copyright 2013 - 2025 The Original Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */

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
            return classCache.computeIfAbsent(className, CachingClassLoadingHelper::loadClass);
        } catch (WrappedException e) {
            throw e.getCause();
        }
    }

    private static Class<?> loadClass(String name) {
        try {
            return Class.forName(name);
        } catch (ClassNotFoundException e) {
            throw new WrappedException(e);
        }
    }

    @Override
    public boolean isCachingEnabled() {
        return true;
    }
}
