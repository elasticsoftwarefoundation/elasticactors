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

package org.elasticsoftware.elasticactors.client;

import org.elasticsoftware.elasticactors.*;
import org.elasticsoftware.elasticactors.cluster.*;

import java.util.concurrent.ThreadLocalRandom;

import static java.lang.String.format;

/**
 * @author Joost van de Wijgerd
 */
public class ClientActorRefTools {
    private static final String EXCEPTION_FORMAT = "Invalid ActorRef, required spec: [actor://<cluster>/<actorSystem>/[shards|nodes|services|clients]/<shardId>/<actorId (optional)>, actual spec: [%s]";
    private final ActorSystemClients actorSystemClients;

    public ClientActorRefTools(ActorSystemClients actorSystemClients) {
        this.actorSystemClients = actorSystemClients;
    }

    public final ActorRef parse(String refSpec) {
        // refSpec should look like: actor://<cluster>/<actorSystem>/shards/<shardId>/<actorId>
        if (refSpec.startsWith("actor://")) {
            int actorSeparatorIndex = 8;
            for (int i = 0; i < 3; i++) {
                int nextIndex = refSpec.indexOf('/', actorSeparatorIndex + 1);
                if (nextIndex == -1) {
                    throw new IllegalArgumentException(
                            format(EXCEPTION_FORMAT, refSpec));
                } else {
                    actorSeparatorIndex = nextIndex;
                }
            }
            int nextIndex = refSpec.indexOf('/', actorSeparatorIndex + 1);
            String actorId = (nextIndex == -1) ? null : refSpec.substring(nextIndex + 1);
            actorSeparatorIndex = (nextIndex == -1) ? actorSeparatorIndex : nextIndex;
            String[] components = (actorId == null) ? refSpec.substring(8).split("/") : refSpec.substring(8, actorSeparatorIndex).split("/");

            String clusterName = components[0];

            return null;

        } else {
            throw new IllegalArgumentException(format(EXCEPTION_FORMAT, refSpec));
        }

    }


}
