package org.elasticsoftware.elasticactors.cluster;

import org.elasticsoftware.elasticactors.serialization.SerializationFramework;

public interface SerializationFrameworkRegistry {
    SerializationFramework getSerializationFramework(Class<? extends SerializationFramework> frameworkClass);
}
