/*
 * Copyright 2013 - 2014 The Original Authors
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

package org.elasticsoftware.elasticactors.cassandra.messaging.journal;

import me.prettyprint.cassandra.service.template.ColumnFamilyResult;
import me.prettyprint.cassandra.service.template.ColumnFamilyRowMapper;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ColumnFamilyUpdater;
import org.elasticsoftware.elasticactors.messaging.CommitLog;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

/**
 * @author Joost van de Wijgerd
 */
public final class CassandraCommitLog implements CommitLog {
    private ColumnFamilyTemplate<String,com.eaio.uuid.UUID> columnFamilyTemplate;
    private final CommitLogEntryColumnMapper columnMapper = new CommitLogEntryColumnMapper();

    @Override
    public void append(String segment, UUID messageId, byte[] data) {
        // convert to other UUID impl

        ColumnFamilyUpdater<String,com.eaio.uuid.UUID> updater = columnFamilyTemplate.createUpdater(segment);
        updater.setByteArray(new com.eaio.uuid.UUID(messageId.getMostSignificantBits(),messageId.getLeastSignificantBits()),
                             data);
        columnFamilyTemplate.update(updater);
    }

    @Override
    public void delete(String segment, UUID messageId) {
        columnFamilyTemplate.deleteColumn(segment,new com.eaio.uuid.UUID(messageId.getMostSignificantBits(),messageId.getLeastSignificantBits()));
    }

    @Override
    public List<CommitLogEntry> replay(String segment) {
        //@todo: make sure they are in the correct order
        return columnFamilyTemplate.queryColumns(segment,columnMapper);
    }

    @Inject
    public void setColumnFamilyTemplate(@Named("messageQueuesColumnFamilyTemplate") ColumnFamilyTemplate<String, com.eaio.uuid.UUID> columnFamilyTemplate) {
        this.columnFamilyTemplate = columnFamilyTemplate;
    }

    private static final class CommitLogEntryColumnMapper implements ColumnFamilyRowMapper<String,com.eaio.uuid.UUID,List<CommitLogEntry>> {

        @Override
        public List<CommitLogEntry> mapRow(final ColumnFamilyResult<String, com.eaio.uuid.UUID> results) {
            List<CommitLogEntry> entries = new LinkedList<CommitLogEntry>();
            if(results.hasResults()) {
                Collection<com.eaio.uuid.UUID> messageIds = results.getColumnNames();
                for(com.eaio.uuid.UUID messageId : messageIds) {
                    entries.add(new CommitLogEntry(UUID.fromString(messageId.toString()), results.getByteArray(messageId)));
                }
            }
            return entries;

        }
    }
}
