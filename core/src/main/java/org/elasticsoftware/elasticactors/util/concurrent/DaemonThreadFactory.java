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

package org.elasticsoftware.elasticactors.util.concurrent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Joost van de Wijgerd
 */
public final class DaemonThreadFactory implements ThreadFactory {

    private final static Logger logger = LoggerFactory.getLogger(DaemonThreadFactory.class);

    private final String name;
    private final AtomicInteger threadCount = new AtomicInteger(0);
    private final UncaughtExceptionHandler uncaughtExceptionHandler;

    public DaemonThreadFactory(String name) {
        this(name, logger);
    }

    public DaemonThreadFactory(String name, Logger logger) {
        this(name, (t, e) -> logger.error("Uncaught exception thrown", e));
    }

    public DaemonThreadFactory(String name, UncaughtExceptionHandler uncaughtExceptionHandler) {
        this.name = name;
        this.uncaughtExceptionHandler = uncaughtExceptionHandler;
    }

    @Override
    public Thread newThread(Runnable r) {
        Thread t = new Thread(r);
        t.setDaemon(true);
        t.setName(String.format("%s-%d", name, threadCount.incrementAndGet()));
        t.setUncaughtExceptionHandler(uncaughtExceptionHandler);
        return t;
    }

    @Override
    public String toString() {
        return name;
    }
}

