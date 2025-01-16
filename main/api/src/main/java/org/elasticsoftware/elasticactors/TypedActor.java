/*
 * Copyright 2013 - 2025 The Original Authors
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

package org.elasticsoftware.elasticactors;

import org.elasticsoftware.elasticactors.serialization.SerializationFramework;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * @author Joost van de Wijgerd
 */
public abstract class TypedActor<T> implements ElasticActor<T> {

    private final DefaultSubscriber defaultSubscriber = new DefaultSubscriber();

    protected final Logger logger = initLogger();

    /**
     * Method used to initialize this actor's logger. This is used to optimize actor classes that
     * create many instances. This is usually not the case for user-created actors, but it can be
     * for many actors created by the framework (such as temporary actors).
     *
     * If that's not the case, there is no need to override this method.
     * Therefore, a default implementation is provided.
     */
    protected Logger initLogger() {
        return LoggerFactory.getLogger(getClass());
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

    @Override
    public Subscriber asSubscriber(@Nullable Class messageClass) {
        // as this can be misused, we need to make sure the class that extends the TypedActor is actually annotated
        // with the Actor annotation, if not this is a programmer error and we'll throw an IllegalStateException
        if(getClass().getAnnotation(Actor.class) != null) {
            return defaultSubscriber;
        } else {
            throw new IllegalStateException("asSubscriber can only be called on a TypedActor implementation that is annotated with the @Actor annotation");
        }
    }

    protected class DefaultSubscriber extends TypedSubscriber<T> {

        protected DefaultSubscriber() {
        }

        @Override
        public void onSubscribe(Subscription s) {
            // start the flow 
            s.request(Long.MAX_VALUE);
        }

        @Override
        public void onNext(T message) {
            // delegate to onReceive
            try {
                onReceive(getPublisher(), message);
            } catch (Exception e) {
                // wrap it in a runtime exception and let the higher level decide what to do
                throw new RuntimeException(e);
            }
        }

        @Override
        public void onError(Throwable t) {
            if(t instanceof PublisherNotFoundException) {
                logger.error("Publisher does not exist, if you want to handle this case please provide your own TypedSubscriber implementation");
            } else {
                logger.error("Unexpected error in TypedActor.DefaultSubscriber", t);
            }
        }

        @Override
        public void onComplete() {
            // do nothing
        }
    }

    // Provide internal access to state etc
    protected final ActorRef getSelf() {
        return ActorContextHolder.getSelf();
    }

    protected <C extends ActorState> C getState(Class<C> stateClass) {
        return ActorContextHolder.getState(stateClass);
    }

    protected final ActorSystem getSystem() {
        return ActorContextHolder.getSystem();
    }

    protected final Collection<PersistentSubscription> getSubscriptions() {
        return ActorContextHolder.getSubscriptions();
    }

    protected final Map<String, Set<ActorRef>> getSubscribers() {
        return ActorContextHolder.getSubscribers();
    }
}
