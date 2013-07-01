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

package org.elasticsoftware.elasticactors.geoevents;

import ch.hsr.geohash.GeoHash;
import org.codehaus.jackson.Version;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.module.SimpleModule;
import org.elasticsoftware.elasticactors.ActorSystem;
import org.elasticsoftware.elasticactors.geoevents.serialization.JacksonGeoHashDeserializer;
import org.elasticsoftware.elasticactors.geoevents.serialization.JacksonGeoHashSerializer;
import org.elasticsoftware.elasticactors.base.SpringBasedActorSystem;
import org.springframework.context.ApplicationContext;


/**
 * @author Joost van de Wijgerd
 */
public final class GeoEventsActorSystem extends SpringBasedActorSystem {
    private final String name;
    private final int numberOfShards;

    public GeoEventsActorSystem() {
        this("GeoEvents",8);
    }

    public GeoEventsActorSystem(String name, int numberOfShards) {
        super("geoevents-beans.xml");
        this.name = name;
        this.numberOfShards = numberOfShards;
    }


    @Override
    protected void doInitialize(ApplicationContext applicationContext, ActorSystem actorSystem) {
        ObjectMapper objectMapper = applicationContext.getBean(ObjectMapper.class);
        // register jackson module for GeoHash ser/de
        objectMapper.registerModule(
                new SimpleModule("GeoEventsModule",new Version(0,1,0,"SNAPSHOT"))
                        .addSerializer(GeoHash.class, new JacksonGeoHashSerializer())
                        .addDeserializer(GeoHash.class, new JacksonGeoHashDeserializer()));
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public int getNumberOfShards() {
        return numberOfShards;
    }
}
