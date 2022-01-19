/*
 *   Copyright 2013 - 2022 The Original Authors
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

package org.elasticsoftware.elasticactors.rabbitmq;

import com.rabbitmq.client.ShutdownListener;
import com.rabbitmq.client.ShutdownSignalException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class LoggingShutdownListener implements ShutdownListener {

    private LoggingShutdownListener() {
    }

    public final static LoggingShutdownListener INSTANCE = new LoggingShutdownListener();

    private final static Logger logger = LoggerFactory.getLogger(LoggingShutdownListener.class);

    @Override
    public void shutdownCompleted(ShutdownSignalException e) {
        if (!e.isInitiatedByApplication()) {
            logger.error("Channel shutdown detected", e);
        }
    }
}
