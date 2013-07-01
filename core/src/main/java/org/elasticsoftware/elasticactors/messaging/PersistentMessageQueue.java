/*
 * Copyright 2013 the original authors
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

package org.elasticsoftware.elasticactors.messaging;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Configurable;

/**
 * @author Joost van de Wijgerd
 */
@Configurable
public abstract class PersistentMessageQueue implements MessageQueue {
    protected final Logger logger = Logger.getLogger(getClass());
    private final String name;
    protected CommitLog commitLog;

    protected PersistentMessageQueue(String name) {
        this.name = name;
    }

    @Override
    public final boolean offer(org.elasticsoftware.elasticactors.messaging.InternalMessage message) {
        try {
            if(message.isDurable()) {
                commitLog.append(name, message.getId(), message.toByteArray());
            }
            doOffer(message);
        } catch (Exception e) {
            logger.error("Exception on offer", e);
            return false;
        }
        return true;
    }

    @Override
    public final String getName() {
        return name;
    }

    protected final void ack(org.elasticsoftware.elasticactors.messaging.InternalMessage message) {
        if(message.isDurable()) {
            commitLog.delete(name,message.getId());
        }
    }

    protected abstract void doOffer(org.elasticsoftware.elasticactors.messaging.InternalMessage message);

    @Autowired
    public final void setCommitLog(CommitLog commitLog) {
        this.commitLog = commitLog;
    }
}
