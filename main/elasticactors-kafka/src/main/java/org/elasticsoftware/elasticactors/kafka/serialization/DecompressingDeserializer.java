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

package org.elasticsoftware.elasticactors.kafka.serialization;

import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import org.elasticsoftware.elasticactors.serialization.Deserializer;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * @author Joost van de Wijgerd
 */
public final class DecompressingDeserializer<O> implements Deserializer<byte[],O> {
    private static final LZ4FastDecompressor lz4Decompressor = LZ4Factory.fastestJavaInstance().fastDecompressor();
    private static final byte[] MAGIC_HEADER = {0x18,0x4D,0x22,0x04};
    private final Deserializer<ByteBuffer,O> delegate;

    public DecompressingDeserializer(Deserializer<ByteBuffer, O> delegate) {
        this.delegate = delegate;
    }

    @Override
    public O deserialize(byte[] serializedBuffer) throws IOException {
        if(isCompressed(serializedBuffer)) {
            ByteBuffer buffer = ByteBuffer.wrap(serializedBuffer);
            // skip the header
            buffer.position(MAGIC_HEADER.length);
            int uncompressedLength = buffer.getInt();
            buffer.rewind();
            ByteBuffer destination = ByteBuffer.allocate(uncompressedLength);
            lz4Decompressor.decompress(
                buffer,
                MAGIC_HEADER.length + Integer.BYTES,
                destination,
                0,
                uncompressedLength
            );
            return delegate.deserialize(destination);
        } else {
            return delegate.deserialize(ByteBuffer.wrap(serializedBuffer));
        }
    }

    @Override
    public boolean isSafe() {
        return delegate.isSafe();
    }

    private boolean isCompressed(byte[] bytes) {
        if (bytes.length < MAGIC_HEADER.length) {
            return false;
        }
        for (int i = 0; i < MAGIC_HEADER.length; i++) {
            if (bytes[i] != MAGIC_HEADER[i]) {
                return false;
            }
        }
        return true;
    }
}
