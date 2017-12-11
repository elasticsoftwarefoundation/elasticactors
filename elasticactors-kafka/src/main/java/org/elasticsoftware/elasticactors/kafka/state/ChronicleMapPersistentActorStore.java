package org.elasticsoftware.elasticactors.kafka.state;

import net.openhft.chronicle.map.ChronicleMap;

public final class ChronicleMapPersistentActorStore {
    private final ChronicleMap<String, byte[]> backingMap;

    public ChronicleMapPersistentActorStore() {
        backingMap = ChronicleMap.of(String.class, byte[].class)
                .create();
    }
}
