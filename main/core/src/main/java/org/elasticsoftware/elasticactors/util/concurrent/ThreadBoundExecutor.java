/*
 * Copyright 2013 - 2023 The Original Authors
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

package org.elasticsoftware.elasticactors.util.concurrent;

/**
 * ThreadBoundExecutor
 *
 * <p>
 * A thread bound executor guarantees that a runnable executed on the executor that has the same key
 * will always be executed by the same thread.
 *
 * @param <T> The type of the key
 * @author Joost van de Wijgerd
 */
public interface ThreadBoundExecutor<T extends ThreadBoundEvent<?>> {

    void execute(T runnable);

    void shutdown();

    int getThreadCount();

    void init();
}
