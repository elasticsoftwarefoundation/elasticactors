package org.elasticsoftware.elasticactors.cassandra.serialization;

import com.google.common.base.Charsets;
import org.elasticsoftware.elasticactors.serialization.Deserializer;

import java.io.IOException;

import static com.google.common.base.Charsets.UTF_8;

/**
 * @author Joost van de Wijgerd
 */
public final class StringDeserializer implements Deserializer<byte[],String> {
    @Override
    public String deserialize(byte[] serializedObject) throws IOException {
        return new String(serializedObject, UTF_8);
    }
}
