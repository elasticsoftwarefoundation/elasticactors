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

package org.elasticsoftware.elasticactors.cassandra2.util;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.*;
import org.elasticsoftware.elasticactors.cassandra2.state.BatchTooLargeException;
import org.slf4j.Logger;

import java.net.InetAddress;
import java.util.Optional;

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
                logger.warn("{} on node {} while executing statement, retry attempt {}", e.getClass().getSimpleName(), e.getEndPoint(), attempts);
                latestException = e;
            }  catch(UnavailableException e) {
                logger.error("node {} is reporting not enough replicas available, will not retry",
                        Optional.ofNullable(e.getEndPoint()).map(endPoint -> endPoint.resolve().toString()).orElse("UNKNOWN"));
                throw e;
            } catch(InvalidQueryException e) {
                logger.error("InvalidQueryException with message {} on node {} while executing statement, will be handled specially in case of BatchStatement",
                        e.getMessage(),
                        Optional.of(e.getEndPoint()).map(endPoint -> endPoint.resolve().toString()).orElse("UNKNOWN"));
                if(statement instanceof BatchStatement batch) {
                    Configuration cassandraConfiguration = cassandraSession.getCluster().getConfiguration();
                    throw new BatchTooLargeException(batch,batch.requestSizeInBytes(
                            cassandraConfiguration.getProtocolOptions().getProtocolVersion(),
                            cassandraConfiguration.getCodecRegistry() ));
                } else {
                    throw e;
                }
            } catch(RuntimeException e) {
                InetAddress node = (e instanceof CoordinatorException) ?
                        Optional.ofNullable(((CoordinatorException)e).getEndPoint())
                                .map(endPoint -> endPoint.resolve().getAddress()).orElse(null)
                        : null;
                logger.error("{} on node {} while executing statement, will not retry", e.getClass().getSimpleName(), node);
                throw e;
            }
        }
        // if we end up here the retry failed and latestException must be set
        logger.error("Failed to execute Statement after {} attempts, throwing latest exception {}", attempts, latestException.getClass().getSimpleName());
        throw latestException;
    }
}
