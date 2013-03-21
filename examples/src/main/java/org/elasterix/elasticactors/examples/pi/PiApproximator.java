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

package org.elasterix.elasticactors.examples.pi;

import org.apache.log4j.Logger;
import org.codehaus.jackson.Version;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.module.SimpleModule;
import org.elasterix.elasticactors.*;
import org.elasterix.elasticactors.examples.common.*;
import org.elasterix.elasticactors.examples.pi.actors.Listener;
import org.elasterix.elasticactors.examples.pi.actors.Master;
import org.elasterix.elasticactors.examples.pi.messages.Calculate;
import org.elasterix.elasticactors.examples.pi.messages.PiApproximation;
import org.elasterix.elasticactors.examples.pi.messages.Result;
import org.elasterix.elasticactors.examples.pi.messages.Work;
import org.elasterix.elasticactors.serialization.Deserializer;
import org.elasterix.elasticactors.serialization.MessageDeserializer;
import org.elasterix.elasticactors.serialization.MessageSerializer;
import org.elasterix.elasticactors.serialization.Serializer;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Joost van de Wijgerd
 */
public final class PiApproximator implements ActorSystemConfiguration, ActorSystemBootstrapper {
    private static final Logger logger = Logger.getLogger(PiApproximator.class);
    private final String name;
    private final int numberOfShards;
    private final ObjectMapper objectMapper = new ObjectMapper();

    private final Serializer<ActorState, byte[]> actorStateSerializer = new JacksonActorStateSerializer(objectMapper);
    private final Deserializer<byte[], ActorState> actorStateDeserializer = new JacksonActorStateDeserializer(objectMapper);

    private final Map<Class<?>, MessageSerializer<?>> messageSerializers = new HashMap<Class<?>, MessageSerializer<?>>() {{
        put(Calculate.class, new JacksonMessageSerializer<Calculate>(objectMapper));
        put(PiApproximation.class, new JacksonMessageSerializer<PiApproximation>(objectMapper));
        put(Result.class, new JacksonMessageSerializer<Result>(objectMapper));
        put(Work.class, new JacksonMessageSerializer<Work>(objectMapper));
    }};

    private final Map<Class<?>, MessageDeserializer<?>> messageDeserializers = new HashMap<Class<?>, MessageDeserializer<?>>() {{
        put(Calculate.class, new JacksonMessageDeserializer<Calculate>(Calculate.class,objectMapper));
        put(PiApproximation.class, new JacksonMessageDeserializer<PiApproximation>(PiApproximation.class,objectMapper));
        put(Result.class, new JacksonMessageDeserializer<Result>(Result.class,objectMapper));
        put(Work.class, new JacksonMessageDeserializer<Work>(Work.class,objectMapper));
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

    // bootstrapper


    @Override
    public void initialize(ActorSystem actorSystem) throws Exception {
        // register jackson module for Actor ref ser/de
        objectMapper.registerModule(
                new SimpleModule("ElasticActorsModule",new Version(0,1,0,"SNAPSHOT"))
                .addSerializer(ActorRef.class, new JacksonActorRefSerializer())
                .addDeserializer(ActorRef.class, new JacksonActorRefDeserializer(actorSystem.getParent().getActorRefFactory())));
    }

    @Override
    public void create(ActorSystem actorSystem, String... arguments) throws Exception {

        // @todo: make configurable by arguments

        // create listener
        ActorRef listener = actorSystem.actorOf("listener",Listener.class);
        // create master
        Master.State masterState = new Master.State(listener,4,10000,10000);
        ActorRef master = actorSystem.actorOf("master",Master.class,new JacksonActorState(objectMapper,masterState));

        // wait a little for the actors to be created
        Thread.sleep(50);


    }

    @Override
    public void activate(ActorSystem actorSystem) {
        ActorRef listener = actorSystem.actorFor("listener");
        ActorRef master = actorSystem.actorFor("master");

        // tell the master to start calculating
        logger.info("Starting to calculate Pi");
        master.tell(new Calculate(),listener);
    }
}
