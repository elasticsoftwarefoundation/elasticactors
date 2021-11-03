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

package org.elasticsoftware.elasticactors.test;

import org.elasticsoftware.elasticactors.Actor;
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.UntypedActor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Joost van de Wijgerd
 */
@Actor
public final class TestActor extends UntypedActor {

    private final static Logger staticLogger = LoggerFactory.getLogger(TestActor.class);

    @Override
    protected Logger initLogger() {
        return staticLogger;
    }

    @Override
    public void onReceive(ActorRef sender, Object message) throws Exception {
        //To change body of implemented methods use File | Settings | File Templates.
    }
}
