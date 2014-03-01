package org.elasticsoftware.elasticactors.cassandra.serialization;

import com.google.common.io.ByteStreams;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import org.elasticsoftware.elasticactors.serialization.Deserializer;

import java.io.DataInput;
import java.io.IOException;

/**
 * @author Joost van de Wijgerd
 */
public final class DecompressingDeserializer<O> implements Deserializer<byte[],O> {
    private static final LZ4FastDecompressor lz4Decompressor = LZ4Factory.fastestJavaInstance().fastDecompressor();
    private static final byte[] MAGIC_HEADER = {0x18,0x4D,0x22,0x04};
    private final Deserializer<byte[],O> delegate;

    public DecompressingDeserializer(Deserializer<byte[], O> delegate) {
        this.delegate = delegate;
    }

    @Override
    public O deserialize(byte[] serializedObject) throws IOException {
        if(isCompressed(serializedObject)) {
            DataInput dataInput = ByteStreams.newDataInput(serializedObject);
            // skip the header
            dataInput.skipBytes(4);
            int uncompressedLength = dataInput.readInt();
            return delegate.deserialize(lz4Decompressor.decompress(serializedObject,8,uncompressedLength));
        } else {
            return delegate.deserialize(serializedObject);
        }
    }

    private boolean isCompressed(byte[] serializedObject) {
        for (int i = 0; i < MAGIC_HEADER.length; i++) {
            if(serializedObject[i] != MAGIC_HEADER[i]) {
                return false;
            }
        }
        return true;
    }
}
