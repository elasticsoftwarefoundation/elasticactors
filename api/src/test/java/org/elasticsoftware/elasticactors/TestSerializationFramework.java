package org.elasticsoftware.elasticactors;

import org.elasticsoftware.elasticactors.serialization.*;

/**
 * @author Joost van de Wijgerd
 */
public final class TestSerializationFramework implements SerializationFramework {
    @Override
    public void register(Class<?> messageClass) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public <T> MessageSerializer<T> getSerializer(Class<T> messageClass) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public <T> MessageDeserializer<T> getDeserializer(Class<T> messageClass) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Serializer<ActorState, byte[]> getActorStateSerializer(Class<? extends ElasticActor> actorClass) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Deserializer<byte[], ActorState> getActorStateDeserializer(Class<? extends ElasticActor> actorClass) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }
}
