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

package org.elasticsoftware.elasticactors.cassandra4.util;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DriverException;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.servererrors.*;
import com.datastax.oss.protocol.internal.request.Batch;
import org.elasticsoftware.elasticactors.cassandra4.state.BatchTooLargeException;
import org.slf4j.Logger;

import java.util.Optional;

import static java.util.Optional.ofNullable;

/**
 * @author Joost van de Wijgerd
 */
public final class ExecutionUtils {
    private ExecutionUtils() {}

    public static ResultSet executeWithRetry(CqlSession cassandraSession, Statement statement, Logger logger) {
        int attempts = 0;
        DriverException latestException = null;
        while (attempts++ <= 3) {
            try {
                return cassandraSession.execute(statement);
            } catch (OverloadedException | QueryConsistencyException | BootstrappingException e) {
                logger.warn("{} on node {} while executing statement, retry attempt {}",
                        e.getClass().getSimpleName(),
                        ofNullable(e.getExecutionInfo().getCoordinator()).map(node -> node.getEndPoint().resolve().toString()).orElse("UNKNOWN"),
                        attempts);
                latestException = e;
            }  catch(UnavailableException e) {
                logger.error("node {} is reporting not enough replicas available, will not retry",
                        ofNullable(e.getExecutionInfo().getCoordinator()).map(node -> node.getEndPoint().resolve().toString()).orElse("UNKNOWN"));
                throw e;
            } catch(InvalidQueryException e) {
                logger.error("InvalidQueryException with message {} on node {} while executing statement, will retry in case of BatchStatement",
                        e.getMessage(),
                        Optional.of(e.getCoordinator()).map(node -> node.getEndPoint().resolve().toString()).orElse("UNKNOWN"));
                if(statement instanceof BatchStatement batch) {
                    throw new BatchTooLargeException(batch,batch.computeSizeInBytes(cassandraSession.getContext()));
                } else {
                    throw e;
                }
            } catch(CoordinatorException e) {
                logger.error("{} with message {} on node {} while executing statement, will not retry",
                        e.getClass().getSimpleName(),
                        e.getMessage(),
                        ofNullable(e.getExecutionInfo().getCoordinator()).map(node -> node.getEndPoint().resolve().toString()).orElse("UNKNOWN"));
                throw e;
            } catch(RuntimeException e) {
                logger.error("{} on node UNKNOWN while executing statement, will not retry", e.getClass().getSimpleName());
                throw e;
            }
        }
        // if we end up here the retry failed and latestException must be set
        logger.error("Failed to execute Statement after {} attempts, throwing latest exception {}", attempts, latestException.getClass().getSimpleName());
        throw latestException;
    }
}
