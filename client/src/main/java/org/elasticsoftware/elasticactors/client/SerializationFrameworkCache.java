package org.elasticsoftware.elasticactors.client;

import org.elasticsoftware.elasticactors.serialization.SerializationFramework;

import java.util.Map;

public class SerializationFrameworkCache {

    private final Map<Class<? extends SerializationFramework>, SerializationFramework> frameworkMap;

    public SerializationFrameworkCache(
            Map<Class<? extends SerializationFramework>, SerializationFramework> frameworkMap) {
        this.frameworkMap = frameworkMap;
    }

    public SerializationFramework getSerializationFramework(
            Class<? extends SerializationFramework> frameworkClass) {
        return this.frameworkMap.get(frameworkClass);
    }

}
