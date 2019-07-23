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

package org.elasticsoftware.elasticactors.test.state;

import org.elasticsoftware.elasticactors.state.ActorStateUpdate;
import org.elasticsoftware.elasticactors.state.ActorStateUpdateListener;

import java.util.List;

/**
 * @author Joost van de Wijgerd
 */
public final class LoggingActorStateUpdateListener implements ActorStateUpdateListener {
    @Override
    public void onUpdate(List<? extends ActorStateUpdate> updates) {
        updates.forEach(update -> System.out.println(String.format("Got an ActorStateUpdate for actorId: %s", update.getActorRef().getActorId())));
    }
}
