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
