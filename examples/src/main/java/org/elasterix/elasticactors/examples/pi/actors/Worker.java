/*
 * Copyright 2013 Joost van de Wijgerd
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

package org.elasterix.elasticactors.examples.pi.actors;

import org.elasterix.elasticactors.ActorRef;
import org.elasterix.elasticactors.UntypedActor;
import org.elasterix.elasticactors.examples.pi.messages.Result;
import org.elasterix.elasticactors.examples.pi.messages.Work;

/**
 * @author Joost van de Wijgerd
 */
public final class Worker extends UntypedActor {
    public void onReceive(Object message, ActorRef sender) {
        if (message instanceof Work) {
            Work work = (Work) message;
            double result = calculatePiFor(work.getStart(), work.getNrOfElements());
            sender.tell(new Result(result), getSelf());
        } else {
            unhandled(message);
        }
    }

    private double calculatePiFor(int start, int nrOfElements) {
        double acc = 0.0;
        for (int i = start * nrOfElements; i <= ((start + 1) * nrOfElements - 1); i++) {
            acc += 4.0 * (1 - (i % 2) * 2) / (2 * i + 1);
        }
        return acc;
    }
}
