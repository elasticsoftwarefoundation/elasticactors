/*
 * Copyright 2013 - 2023 The Original Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */

package org.elasticsoftware.elasticactors.cassandra.common.serialization;

import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import org.elasticsoftware.elasticactors.serialization.Deserializer;
import org.elasticsoftware.elasticactors.util.ByteBufferUtils;

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
            ByteBuffer uncompressed = ByteBufferUtils.doAndReset(
                serializedBuffer, 
                DecompressingDeserializer::uncompress
            );
            return delegate.deserialize(uncompressed);
        } else {
            return delegate.deserialize(serializedBuffer);
        }
    }

    @Override
    public boolean isSafe() {
        return delegate.isSafe();
    }

    private static int getUncompressedLength(ByteBuffer buffer) {
        // skip the header
        buffer.position(MAGIC_HEADER.length);
        return buffer.getInt();
    }

    private static ByteBuffer uncompress(ByteBuffer buffer) {
        int uncompressedLength = ByteBufferUtils.toIntAndReset(
            buffer,
            DecompressingDeserializer::getUncompressedLength
        );
        ByteBuffer destination = ByteBuffer.allocate(uncompressedLength);
        lz4Decompressor.decompress(
            buffer,
            MAGIC_HEADER.length + Integer.BYTES,
            destination,
            0,
            uncompressedLength
        );
        destination.rewind();
        return destination;
    }

    private static boolean isCompressed(byte[] bytes) {
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

    private static boolean isCompressed(ByteBuffer bytes) {
        if (bytes.hasArray()) {
            return isCompressed(bytes.array());
        } else {
            if (bytes.remaining() < MAGIC_HEADER.length) {
                return false;
            }
            return ByteBufferUtils.testAndReset(
                bytes,
                DecompressingDeserializer::isCompressedBuffer
            );
        }
    }

    private static boolean isCompressedBuffer(ByteBuffer copy) {
        for (byte b : MAGIC_HEADER) {
            if (b != copy.get()) {
                return false;
            }
        }
        return true;
    }
}
