package org.elasticsoftware.elasticactors.messaging;

public interface Hasher {

    int hashStringToInt(String value);

    long hashStringToLong(String value);
}
