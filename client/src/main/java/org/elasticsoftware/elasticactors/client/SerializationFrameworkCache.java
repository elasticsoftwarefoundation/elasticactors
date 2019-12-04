package org.elasticsoftware.elasticactors.client;

import org.elasticsoftware.elasticactors.serialization.SerializationFramework;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public class SerializationFrameworkCache {

    private final Map<Class<? extends SerializationFramework>, SerializationFramework>
            frameworkMap = new HashMap<>();
    private final Function<Class<? extends SerializationFramework>, SerializationFramework>
            serializationFrameworkFunction;

    public SerializationFrameworkCache(
            Function<Class<? extends SerializationFramework>, SerializationFramework>
                    serializationFrameworkFunction) {
        this.serializationFrameworkFunction = serializationFrameworkFunction;
    }

    public SerializationFramework getSerializationFramework(
            Class<? extends SerializationFramework> frameworkClass) {
        return this.frameworkMap.computeIfAbsent(frameworkClass, serializationFrameworkFunction);
    }

}
