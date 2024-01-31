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

package org.elasticsoftware.elasticactors.cassandra2.state;

import com.datastax.driver.core.BatchStatement;

public class BatchTooLargeException extends RuntimeException {
    private final BatchStatement originalBatch;
    private final int batchSize;

    public BatchTooLargeException(BatchStatement originalBatch, int batchSize) {
        super("BatchStatement of size "+batchSize+" too large to execute");
        this.originalBatch = originalBatch;
        this.batchSize = batchSize;
    }

    public BatchStatement getOriginalBatch() {
        return originalBatch;
    }

    public int getBatchSize() {
        return batchSize;
    }

}
