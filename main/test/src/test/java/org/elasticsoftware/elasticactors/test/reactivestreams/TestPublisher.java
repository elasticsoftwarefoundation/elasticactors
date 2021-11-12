/*
 *   Copyright 2013 - 2019 The Original Authors
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

package org.elasticsoftware.elasticactors.test.reactivestreams;

import org.elasticsoftware.elasticactors.Actor;
import org.elasticsoftware.elasticactors.ActorSystem;
import org.elasticsoftware.elasticactors.MessageHandler;
import org.elasticsoftware.elasticactors.MethodActor;

import java.util.concurrent.TimeUnit;

/**
 * @author Joost van de Wijgerd
 */
@Actor
public final class TestPublisher extends MethodActor {
    @Override
    public void postActivate(String previousVersion) throws Exception {
        // schedule a message
        Long initialSequence = 1L;
        getSystem().getScheduler()
            .scheduleOnce(
                new StreamedMessage("Streaming #" + initialSequence, initialSequence),
                getSelf(),
                1,
                TimeUnit.SECONDS
            );
    }

    @MessageHandler
    public void handle(StreamedMessage streamedMessage, ActorSystem actorSystem) {
        // reschedule
        Long nextSequence = streamedMessage.getSequenceNumber() + 1L;
        getSystem().getScheduler()
            .scheduleOnce(
                new StreamedMessage("Streaming #" + nextSequence, nextSequence),
                getSelf(),
                1,
                TimeUnit.SECONDS
            );
    }
}
