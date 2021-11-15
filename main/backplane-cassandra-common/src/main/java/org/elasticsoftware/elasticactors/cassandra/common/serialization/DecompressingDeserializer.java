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

import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import org.elasticsoftware.elasticactors.serialization.Deserializer;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * @author Joost van de Wijgerd
 */
public final class DecompressingDeserializer<O> implements Deserializer<ByteBuffer,O> {
    private static final LZ4FastDecompressor lz4Decompressor = LZ4Factory.fastestJavaInstance().fastDecompressor();
    private static final byte[] MAGIC_HEADER = {0x18,0x4D,0x22,0x04};
    private final Deserializer<ByteBuffer,O> delegate;

    public DecompressingDeserializer(Deserializer<ByteBuffer, O> delegate) {
        this.delegate = delegate;
    }

    @Override
    public O deserialize(ByteBuffer serializedBuffer) throws IOException {
        if(isCompressed(serializedBuffer)) {
            ByteBuffer serializedObject = serializedBuffer.asReadOnlyBuffer();
            // skip the header
            serializedObject.position(MAGIC_HEADER.length);
            int uncompressedLength = serializedObject.getInt();
            serializedObject.position(serializedBuffer.position());
            ByteBuffer destination = ByteBuffer.allocate(uncompressedLength);
            lz4Decompressor.decompress(serializedObject, MAGIC_HEADER.length + 4, destination, 0, uncompressedLength);
            return delegate.deserialize(destination);
        } else {
            return delegate.deserialize(serializedBuffer);
        }
    }

    private boolean isCompressed(ByteBuffer serializedObject) {
        if (serializedObject.remaining() < MAGIC_HEADER.length) {
            return false;
        }
        byte[] header = new byte[MAGIC_HEADER.length];
        serializedObject.asReadOnlyBuffer().get(header);
        for (int i = 0; i < MAGIC_HEADER.length; i++) {
            if(header[i] != MAGIC_HEADER[i]) {
                return false;
            }
        }
        return true;
    }
}
