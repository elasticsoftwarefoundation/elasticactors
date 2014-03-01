package org.elasticsoftware.elasticactors.cassandra.serialization;

import com.google.common.base.Charsets;
import org.elasticsoftware.elasticactors.serialization.Serializer;

import java.io.IOException;

import static com.google.common.base.Charsets.UTF_8;

/**
 * @author Joost van de Wijgerd
 */
public final class StringSerializer implements Serializer<String,byte[]> {
    @Override
    public byte[] serialize(String s) throws IOException {
        return s.getBytes(UTF_8);
    }
}
