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
import org.elasterix.elasticactors.examples.pi.messages.PiApproximation;

/**
 * @author Joost van de Wijgerd
 */
public final class Listener extends UntypedActor {
    public void onReceive(Object message, ActorRef sender) {
        if (message instanceof PiApproximation) {
            PiApproximation approximation = (PiApproximation) message;
            System.out.println(String.format("\n\tPi approximation: " +
                    "\t\t%s\n\tCalculation time: \t%d",
                    approximation.getPi(), approximation.getDuration()));
            // @todo: figure out if and how to do this
            // getContext().system().shutdown();
        } else {
            unhandled(message);
        }
    }
}
