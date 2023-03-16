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

package org.elasticsoftware.elasticactors.test.state;

import org.elasticsoftware.elasticactors.state.ActorStateUpdate;
import org.elasticsoftware.elasticactors.state.ActorStateUpdateListener;
import org.elasticsoftware.elasticactors.tracing.MessagingContextManager.MessagingScope;
import org.elasticsoftware.elasticactors.tracing.TraceContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.elasticsoftware.elasticactors.tracing.MessagingContextManager.getManager;

/**
 * @author Joost van de Wijgerd
 */
public final class LoggingActorStateUpdateListener implements ActorStateUpdateListener {

    private static final Logger logger =
            LoggerFactory.getLogger(LoggingActorStateUpdateListener.class);

    @Override
    public void onUpdate(List<? extends ActorStateUpdate> updates) {
        updates.forEach(update -> {
            try (MessagingScope ignored = getManager().enter(
                    new TraceContext(update.getTraceContext()),
                    update.getCreationContext())) {
                logger.info(
                        "Got an ActorStateUpdate for actorId: {}",
                        update.getActorRef().getActorId());
            }
        });
    }
}
