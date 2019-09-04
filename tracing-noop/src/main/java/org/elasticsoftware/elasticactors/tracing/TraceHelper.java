/*
 *   Copyright 2013 - 2019 The Original Authors
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.elasticsoftware.elasticactors.tracing;

import com.google.common.collect.ImmutableMap;
import org.elasticsoftware.elasticactors.messaging.InternalMessage;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.concurrent.Callable;
import java.util.function.Supplier;

public final class TraceHelper {

    public static void runWithTracing(@Nonnull String name, @Nullable InternalMessage message, @Nonnull Runnable runnable) {
        runnable.run();
    }

    public static <T> T callWithTracing(@Nonnull String name, @Nullable InternalMessage message, @Nonnull Callable<T> callable) throws Exception {
        return callable.call();
    }

    public static <T> T supplyWithTracing(@Nonnull String name, @Nullable InternalMessage message, @Nonnull Supplier<T> supplier) {
        return supplier.get();
    }

    public static void throwingRunWithTracing(@Nonnull String name, @Nullable InternalMessage message, @Nonnull ThrowingRunnable throwingRunnable) throws Exception {
        throwingRunnable.run();
    }

    public static void runWithTracing(@Nonnull String name, @Nonnull Runnable runnable) {
        runWithTracing(name, null, runnable);
    }

    public static <T> T callWithTracing(@Nonnull String name, @Nonnull Callable<T> callable) throws Exception {
        return callWithTracing(name, null, callable);
    }

    public static <T> T supplyWithTracing(@Nonnull String name, @Nonnull Supplier<T> supplier) {
        return supplyWithTracing(name, null, supplier);
    }

    public static void throwingRunWithTracing(@Nonnull String name, @Nonnull ThrowingRunnable throwingRunnable) throws Exception {
        throwingRunWithTracing(name, null, throwingRunnable);
    }

    @Nullable
    public static ImmutableMap<String, String> getTraceData() {
        return null;
    }
}
