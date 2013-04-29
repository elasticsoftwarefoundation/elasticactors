package org.elasticsoftware.elasticactors.geoevents;

import ch.hsr.geohash.GeoHash;
import org.codehaus.jackson.Version;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.module.SimpleModule;
import org.elasterix.elasticactors.ActorSystem;
import org.elasticsoftware.elasticactors.geoevents.serialization.JacksonGeoHashDeserializer;
import org.elasticsoftware.elasticactors.geoevents.serialization.JacksonGeoHashSerializer;
import org.elasticsoftwarefoundation.elasticactors.base.SpringBasedActorSystem;
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
                        .addDeserializer(GeoHash.class, new JacksonGeoHashDeserializer(actorSystem.getParent().getActorRefFactory())));
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
