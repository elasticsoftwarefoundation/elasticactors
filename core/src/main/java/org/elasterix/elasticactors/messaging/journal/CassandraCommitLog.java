/*
 * Copyright (c) 2013 Joost van de Wijgerd <jwijgerd@gmail.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.elasterix.elasticactors.messaging.journal;

import me.prettyprint.cassandra.service.template.ColumnFamilyResult;
import me.prettyprint.cassandra.service.template.ColumnFamilyRowMapper;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ColumnFamilyUpdater;
import org.elasterix.elasticactors.messaging.CommitLog;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

/**
 * @author Joost van de Wijgerd
 */
public final class CassandraCommitLog implements CommitLog {
    private ColumnFamilyTemplate<String,UUID> columnFamilyTemplate;
    private final CommitLogEntryColumnMapper columnMapper = new CommitLogEntryColumnMapper();

    @Override
    public void append(String segment, UUID messageId, byte[] data) {
        ColumnFamilyUpdater<String,UUID> updater = columnFamilyTemplate.createUpdater(segment);
        updater.setByteArray(messageId,data);
        columnFamilyTemplate.update(updater);
    }

    @Override
    public void delete(String segment, UUID messageId) {
        columnFamilyTemplate.deleteColumn(segment,messageId);
    }

    @Override
    public List<CommitLogEntry> replay(String segment) {
        //@todo: make sure they are in the correct order
        return columnFamilyTemplate.queryColumns(segment,columnMapper);
    }

    @Autowired
    public void setColumnFamilyTemplate(@Qualifier("messageQueuesColumnFamilyTemplate") ColumnFamilyTemplate<String, UUID> columnFamilyTemplate) {
        this.columnFamilyTemplate = columnFamilyTemplate;
    }

    private static final class CommitLogEntryColumnMapper implements ColumnFamilyRowMapper<String,UUID,List<CommitLogEntry>> {

        @Override
        public List<CommitLogEntry> mapRow(final ColumnFamilyResult<String, UUID> results) {
            List<CommitLogEntry> entries = new LinkedList<CommitLogEntry>();
            if(results.hasResults()) {
                Collection<UUID> messageIds = results.getColumnNames();
                for(UUID messageId : messageIds) {
                    entries.add(new CommitLogEntry(messageId, results.getByteArray(messageId)));
                }
            }
            return entries;

        }
    }
}
