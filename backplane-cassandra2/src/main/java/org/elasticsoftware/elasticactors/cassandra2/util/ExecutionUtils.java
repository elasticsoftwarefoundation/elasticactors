/*
 * Copyright 2013 - 2017 The Original Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.elasticsoftware.elasticactors.cassandra2.util;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.*;
import org.apache.logging.log4j.Logger;

import java.net.InetAddress;

import static java.lang.String.format;

/**
 * @author Joost van de Wijgerd
 */
public final class ExecutionUtils {
    private ExecutionUtils() {}

    public static ResultSet executeWithRetry(Session cassandraSession, Statement statement, Logger logger) {
        int attempts = 0;
        DriverException latestException = null;
        while (attempts++ <= 3) {
            try {
                return cassandraSession.execute(statement);
            } catch (ConnectionException | OverloadedException | QueryConsistencyException | BootstrappingException e) {
                logger.warn(format("%s on node %s while executing statement, retry attempt %d", e.getClass().getSimpleName(), e.getHost(), attempts));
                latestException = e;
            }  catch(UnavailableException e) {
                logger.error(format("node %s is reporting not enough replicas available, will not retry", e.getHost()));
                throw e;
            } catch(RuntimeException e) {
                InetAddress node = (e instanceof CoordinatorException) ? ((CoordinatorException)e).getHost() : null;
                logger.error(format("%s on node %s while executing statement, will not retry", e.getClass().getSimpleName(), node));
                throw e;
            }
        }
        // if we end up here the retry failed and latestException must be set
        logger.error(format("Failed to execute Statement after %d attempts, throwing latest exception %s", attempts, latestException.getClass().getSimpleName()));
        throw latestException;
    }
}
