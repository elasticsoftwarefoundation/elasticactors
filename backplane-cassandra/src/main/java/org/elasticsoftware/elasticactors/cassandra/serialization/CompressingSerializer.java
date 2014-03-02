package org.elasticsoftware.elasticactors.cassandra.serialization;

import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import org.elasticsoftware.elasticactors.serialization.Serializer;

import java.io.IOException;

/**
 * @author Joost van de Wijgerd
 */
public final class CompressingSerializer<I> implements Serializer<I,byte[]> {
    private static final LZ4Compressor lz4Compressor = LZ4Factory.fastestJavaInstance().fastCompressor();
    private static final byte[] MAGIC_HEADER = {0x18,0x4D,0x22,0x04};
    private final Serializer<I,byte[]> delegate;
    private final int compressionThreshold;

    public CompressingSerializer(Serializer<I, byte[]> delegate) {
        this(delegate,2048);
    }

    public CompressingSerializer(Serializer<I, byte[]> delegate, int compressionThreshold) {
        this.delegate = delegate;
        this.compressionThreshold = compressionThreshold;
    }

    @Override
    public byte[] serialize(I object) throws IOException {
        byte[] serializedObject = delegate.serialize(object);
        if(serializedObject.length > compressionThreshold) {
            byte[] compressedBytes =  lz4Compressor.compress(serializedObject);
            ByteArrayDataOutput dataOutput = ByteStreams.newDataOutput(compressedBytes.length+8);
            dataOutput.write(MAGIC_HEADER);
            dataOutput.writeInt(serializedObject.length);
            dataOutput.write(compressedBytes);
            return dataOutput.toByteArray();
        } else {
            return serializedObject;
        }
    }
}
