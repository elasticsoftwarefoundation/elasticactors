/*
 *   Copyright 2013 - 2019 The Original Authors
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.elasticsoftware.elasticactors.cassandra.common.serialization;

import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import org.elasticsoftware.elasticactors.serialization.Serializer;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * @author Joost van de Wijgerd
 */
public final class CompressingSerializer<I> implements Serializer<I,ByteBuffer> {
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
    public ByteBuffer serialize(I object) throws IOException {
        byte[] serializedObject = delegate.serialize(object);
        if(serializedObject.length > compressionThreshold) {
            byte[] compressedBytes =  lz4Compressor.compress(serializedObject);
            ByteArrayDataOutput dataOutput = ByteStreams.newDataOutput(compressedBytes.length+8);
            dataOutput.write(MAGIC_HEADER);
            dataOutput.writeInt(serializedObject.length);
            dataOutput.write(compressedBytes);
            return ByteBuffer.wrap(dataOutput.toByteArray());
        } else {
            return ByteBuffer.wrap(serializedObject);
        }
    }
}
