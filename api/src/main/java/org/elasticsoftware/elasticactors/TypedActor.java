/*
 * Copyright 2013 - 2016 The Original Authors
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

package org.elasticsoftware.elasticactors;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsoftware.elasticactors.serialization.SerializationFramework;

/**
 * @author Joost van de Wijgerd
 */
public abstract class TypedActor<T> implements ElasticActor<T> {

    @Override
    public <S extends ActorState> S preCreate(Object input) {
        return null;
    }

    @Override
    public void postCreate(ActorRef creator) throws Exception {
        // do nothing by default
    }

    @Override
    public ActorState preActivate(String previousVersion, String currentVersion, byte[] serializedForm, SerializationFramework serializationFramework) throws Exception {
        // do nothing by default
        return null;
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

    protected <S extends ActorState> S getState(Class<S> stateClass) {
        return ActorContextHolder.getState(stateClass);
    }

    protected final ActorSystem getSystem() {
        return ActorContextHolder.getSystem();
    }

    protected final void unhandled(Object message) {
        //@todo: implement logic for unhandled messages
    }

}
