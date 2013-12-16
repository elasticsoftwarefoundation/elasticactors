/*
 * Copyright 2013 the original authors
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

package org.elasticsoftware.elasticactors.examples.pi;

import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.apache.log4j.Logger;

import org.elasticsoftware.elasticactors.*;
import org.elasticsoftware.elasticactors.examples.pi.actors.Listener;
import org.elasticsoftware.elasticactors.examples.pi.actors.Master;
import org.elasticsoftware.elasticactors.examples.pi.messages.Calculate;
import org.elasticsoftware.elasticactors.examples.pi.messages.PiApproximation;
import org.elasticsoftware.elasticactors.examples.pi.messages.Result;
import org.elasticsoftware.elasticactors.examples.pi.messages.Work;
import org.elasticsoftware.elasticactors.serialization.Deserializer;
import org.elasticsoftware.elasticactors.serialization.MessageDeserializer;
import org.elasticsoftware.elasticactors.serialization.MessageSerializer;
import org.elasticsoftware.elasticactors.serialization.Serializer;
import org.elasticsoftware.elasticactors.base.serialization.*;
import org.elasticsoftware.elasticactors.base.state.JacksonActorStateFactory;
import org.elasticsoftware.elasticactors.http.messages.HttpRequest;
import org.elasticsoftware.elasticactors.http.messages.HttpResponse;

import java.util.*;

/**
 * @author Joost van de Wijgerd
 */
@DependsOn(dependencies = {"Http"})
public final class PiApproximator implements ActorSystemConfiguration, ActorSystemBootstrapper {
    private static final Logger logger = Logger.getLogger(PiApproximator.class);
    private final String name;
    private final int numberOfShards;
    private final ObjectMapper objectMapper = new ObjectMapper();

    private final ActorStateFactory actorStateFactory = new JacksonActorStateFactory(objectMapper);
    private final Serializer<ActorState, byte[]> actorStateSerializer = new JacksonActorStateSerializer(objectMapper);
    private final Deserializer<byte[], ActorState> actorStateDeserializer = new JacksonActorStateDeserializer(objectMapper, actorStateFactory);


    // the listener service to dispatch http requests
    private final Listener listenerService = new Listener(objectMapper);

    private final Map<Class<?>, MessageSerializer<?>> messageSerializers = new HashMap<Class<?>, MessageSerializer<?>>() {{
        put(Calculate.class, new JacksonMessageSerializer<Calculate>(objectMapper));
        put(PiApproximation.class, new JacksonMessageSerializer<PiApproximation>(objectMapper));
        put(Result.class, new JacksonMessageSerializer<Result>(objectMapper));
        put(Work.class, new JacksonMessageSerializer<Work>(objectMapper));
        put(HttpResponse.class,new JacksonMessageSerializer<HttpResponse>(objectMapper));
        put(HttpRequest.class,new JacksonMessageSerializer<HttpRequest>(objectMapper));
    }};

    private final Map<Class<?>, MessageDeserializer<?>> messageDeserializers = new HashMap<Class<?>, MessageDeserializer<?>>() {{
        put(Calculate.class, new JacksonMessageDeserializer<Calculate>(Calculate.class,objectMapper));
        put(PiApproximation.class, new JacksonMessageDeserializer<PiApproximation>(PiApproximation.class,objectMapper));
        put(Result.class, new JacksonMessageDeserializer<Result>(Result.class,objectMapper));
        put(Work.class, new JacksonMessageDeserializer<Work>(Work.class,objectMapper));
        put(HttpRequest.class, new JacksonMessageDeserializer<HttpRequest>(HttpRequest.class,objectMapper));
        put(HttpResponse.class, new JacksonMessageDeserializer<HttpResponse>(HttpResponse.class,objectMapper));
    }};

    public PiApproximator(String name, int numberOfShards) {
        this.name = name;
        this.numberOfShards = numberOfShards;

    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public int getNumberOfShards() {
        return numberOfShards;
    }

    @Override
    public String getVersion() {
        return "0.1.0";
    }

    @Override
    public <T> MessageSerializer<T> getSerializer(Class<T> messageClass) {
        return (MessageSerializer<T>) messageSerializers.get(messageClass);
    }

    @Override
    public <T> MessageDeserializer<T> getDeserializer(Class<T> messageClass) {
        return (MessageDeserializer<T>) messageDeserializers.get(messageClass);
    }

    @Override
    public Serializer<ActorState, byte[]> getActorStateSerializer() {
        return actorStateSerializer;
    }

    @Override
    public Deserializer<byte[], ActorState> getActorStateDeserializer() {
        return actorStateDeserializer;
    }

    @Override
    public ActorStateFactory getActorStateFactory() {
        return actorStateFactory;
    }

    @Override
    public ElasticActor getService(String serviceId) {
        if("pi/calculate".equals(serviceId)) {
            return listenerService;
        } else {
            return null;
        }
    }

    @Override
    public Set<String> getServices() {
        return new HashSet<String>(Arrays.asList("pi/calculate"));
    }

    // bootstrapper


    @Override
    public void initialize(ActorSystem actorSystem, Properties properties) throws Exception {
        // register jackson module for Actor ref ser/de
        objectMapper.registerModule(
                new SimpleModule("ElasticActorsModule",new Version(0,1,0,"SNAPSHOT"))
                .addSerializer(ActorRef.class, new JacksonActorRefSerializer())
                .addDeserializer(ActorRef.class, new JacksonActorRefDeserializer(actorSystem.getParent().getActorRefFactory())));
    }

    @Override
    public void create(ActorSystem actorSystem, String... arguments) throws Exception {
        logger.info("PiApproximator.create");
        // @todo: make configurable by arguments

        // get listener ref (this is now a service actor)
        ActorRef listener = actorSystem.serviceActorFor("pi/calculate");
        logger.info(String.format("listener ref: %s",listener));

        // create master
        Master.State masterState = new Master.State(listener,4,10000,10000);
        ActorRef master = actorSystem.actorOf("master",Master.class,masterState);

        // wait a little for the actors to be created
        Thread.sleep(50);


    }

    @Override
    public void activate(ActorSystem actorSystem) {

    }

    @Override
    public void destroy() throws Exception {

    }
}
