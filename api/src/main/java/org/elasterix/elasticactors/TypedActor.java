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

package org.elasterix.elasticactors;


import org.apache.log4j.Logger;

/**
 * @author Joost van de Wijgerd
 */
public abstract class TypedActor<T> implements ElasticActor<T> {
    private final Logger logger = Logger.getLogger(getClass());

    @Override
    public void postCreate(ActorRef creator) throws Exception {
        // do nothing by default
    }

    @Override
    public void postActivate(String previousVersion) throws Exception {
        // do nothing by default
    }

    @Override
    public void onUndeliverable(ActorRef receiver, Object message) throws Exception {
        // do nothing by default
    }

    @Override
    public void prePassivate() throws Exception {
        // do nothing by default
    }

    @Override
    public void preDestroy(ActorRef destroyer) throws Exception {
        // do nothing by default
    }

    // Provide internal access to state etc
    protected final ActorRef getSelf() {
        return ActorContextHolder.getSelf();
    }

    protected final ActorState getState(ActorStateFactory actorStateFactory) {
        return ActorContextHolder.getState(actorStateFactory);
    }

    protected final ActorSystem getSystem() {
        return ActorContextHolder.getSystem();
    }

    protected final void unhandled(Object message) {
        //@todo: implement logic for unhandled messages
    }

}
