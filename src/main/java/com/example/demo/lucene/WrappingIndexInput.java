package com.example.demo.lucene;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;
import lombok.SneakyThrows;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

public class WrappingIndexInput extends IndexInput {

    private Directory directory;

    private IOContext context;

    private String name;

    private WrappingDecoder decoder;

    private RangeMap<Long, BufferRange> bufferRanges;

    private Cache<BufferRange, byte[]> decryptedBuffers;

    private final int bufferSize;

    private long startPosition;

    private long limitPosition;

    private long currentPosition;

    public WrappingIndexInput(Directory directory, String name, IOContext context, WrappingDecoder decoder, int bufferSize) throws IOException {
        super("WrappingIndexInput(\"" + name + "\"");

        // Copy directory
        this.directory = directory;

        // Copy context
        this.context = context;

        // Copy name
        this.name = name;

        // Create original
        IndexInput original = this.directory.openInput(name, context);

        // Copy decoder
        this.decoder = decoder;

        // Copy buffer size
        this.bufferSize = bufferSize;

        // Set start position
        this.startPosition = 0;

        // Set current position
        this.currentPosition = 0;

        // Create map of buffer ranges
        this.bufferRanges = TreeRangeMap.create();

        // Create cache of decrypted buffers
        this.decryptedBuffers = CacheBuilder.newBuilder()
                .expireAfterAccess(5, TimeUnit.SECONDS)
                .build();

        // Get length of IndexInput
        long length = original.length();

        // Position in file
        long position = 0;

        // If length is greater than zero
        if (length > 0) {

            // Read limit position
            this.limitPosition = original.readLong();

            // Increment position by 8
            position += 8;

            // Decrement length by 8
            length -= 8;

            // While there is still data to read
            for (int i = 0; length > 0; i++) {

                // Read encoded chunk length from IndexInput
                int encodedLength = original.readInt();

                // Read decoded chunk length from IndexInput
                int decodedLength = original.readInt();

                // Increment position by 8 bytes
                position += 8;

                // Decrease length by 8 bytes
                length -= 8;

                // Compute start of decoded chunk
                long start = i * bufferSize;

                // Compute end of decoded chunk
                long end = start + decodedLength - 1;

                // Create buffer range
                BufferRange bufferRange = BufferRange.builder()
                        .encodedPosition(position)
                        .decodedPosition(start)
                        .encodedLength(encodedLength)
                        .build();

                // Add buffer range to map
                bufferRanges.put(Range.closed(start, end), bufferRange);

                // Skip chunk length bytes
                original.skipBytes(encodedLength);

                // Increment position by chunk length bytes
                position += encodedLength;

                // Decrease length by chunk length bytes
                length -= encodedLength;
            }

        }

        // Close original
        original.close();

    }

    public WrappingIndexInput(WrappingIndexInput wrappingIndexInput) throws IOException {
        super(wrappingIndexInput.toString());

        // Copy directory
        this.directory = wrappingIndexInput.directory;

        // Copy context
        this.context = wrappingIndexInput.context;

        // Copy name
        this.name = wrappingIndexInput.name;

        this.decoder = wrappingIndexInput.decoder;
        this.bufferRanges = wrappingIndexInput.bufferRanges;
        this.decryptedBuffers = CacheBuilder.newBuilder()
                .expireAfterAccess(5, TimeUnit.SECONDS)
                .build();
        this.bufferSize = wrappingIndexInput.bufferSize;
        this.startPosition = wrappingIndexInput.startPosition;
        this.limitPosition = wrappingIndexInput.limitPosition;
        this.currentPosition = wrappingIndexInput.currentPosition;
    }

    @Override
    public void close() throws IOException {
        // Do nothing
    }

    @Override
    public long getFilePointer() {
        return this.currentPosition;
    }

    public long getRealFilePointer() {
        return this.currentPosition + this.startPosition;
    }

    @Override
    public void seek(long pos) throws IOException {
        // If position plus start position is greater than limit position
        if (pos + this.startPosition > this.limitPosition) {

            // Throw exception
            throw new IOException("Cannot seek to position " + pos + " because it is beyond the limit position " + (this.limitPosition - this.startPosition));
        }
        this.currentPosition = pos;
    }

    @Override
    public long length() {
        return this.limitPosition - this.startPosition;
    }

    @Override
    public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
        // Clone this object
        WrappingIndexInput wrappingIndexInput = new WrappingIndexInput(this);

        // Set start position
        wrappingIndexInput.startPosition = this.startPosition + offset;

        // Set limit position
        wrappingIndexInput.limitPosition = this.startPosition + offset + length;

        // Set current position
        wrappingIndexInput.currentPosition = 0;

        // Return wrapped IndexInput
        return wrappingIndexInput;
    }

    @Override
    public byte readByte() throws IOException {

        // If current position plus one is greater than limit position
        if (this.getRealFilePointer() + 1 > this.limitPosition) {

            // Throw exception
            throw new IOException("Buffer range not found for position " + this.getFilePointer() + " " + this.getRealFilePointer() + "/" + this.limitPosition);
        }

        // Get buffer range
        BufferRange bufferRange = this.bufferRanges.get(this.getRealFilePointer());

        // If buffer range is not null
        if (bufferRange != null) {

            // Check if buffer is cached
            byte[] decodedChunk = this.getDecodedChunk(bufferRange);

            // Create byte buffer for decoded chunk
            ByteBuffer byteBuffer = ByteBuffer.wrap(decodedChunk);

            // Seek to buffer range position
            byteBuffer.position((int) (this.getRealFilePointer() - bufferRange.getDecodedPosition()));

            // Read byte from byte buffer
            byte b = byteBuffer.get();

            // Increment current position
            this.currentPosition++;

            // Return byte
            return b;

        }

        // Throw exception
        throw new IOException("Buffer range not found for position " + this.getFilePointer());
    }

    @Override
    public void readBytes(byte[] b, int offset, int len) throws IOException {

        // If current position plus length is greater than limit position
        if (this.getRealFilePointer() + len > this.limitPosition) {

            // Throw exception
            throw new IOException("Buffer range not found for position " + this.getFilePointer());
        }

        // Copy length
        int length = len;

        // While length is greater than zero
        while (length > 0) {

            // Get buffer range
            BufferRange bufferRange = this.bufferRanges.get(this.getRealFilePointer());

            // If buffer range is not null
            if (bufferRange != null) {

                // Check if buffer is cached
                byte[] decodedChunk = this.getDecodedChunk(bufferRange);

                // Compute copy offset
                int copyOffset = (int) (this.getRealFilePointer() - bufferRange.getDecodedPosition());

                // Minimum of length and buffer range length
                int minLength = Math.min(length, decodedChunk.length - copyOffset);

                // Copy length bytes from decoded chunk to b
                System.arraycopy(decodedChunk, copyOffset, b, offset, minLength);

                // Increment current position
                this.currentPosition += minLength;

                // Decrement length
                length -= minLength;

                // Increment offset
                offset += minLength;

            } else {

                // Throw exception
                throw new IOException("Buffer range not found for position " + this.getFilePointer());

            }

        }

    }

    public byte [] getDecodedChunk(BufferRange bufferRange) throws IOException {

        // Check if buffer is cached
        byte[] decodedChunk = this.decryptedBuffers.getIfPresent(bufferRange);

        // If buffer is null
        if (decodedChunk == null) {

            // Open IndexInput
            IndexInput indexInput = this.directory.openInput(this.name, this.context);

            // Seek to buffer range position
            indexInput.seek(bufferRange.getEncodedPosition());

            // Create byte array for encoded chunk
            byte [] encodedChunk = new byte[bufferRange.getEncodedLength()];

            // Read encoded chunk
            indexInput.readBytes(encodedChunk, 0, encodedChunk.length);

            // Close IndexInput
            indexInput.close();

            // Decode chunk
            decodedChunk = this.decoder.decode(encodedChunk);

            // Add buffer to cache
            this.decryptedBuffers.put(bufferRange, decodedChunk);

        }

        return decodedChunk;

    }

    @SneakyThrows
    @Override
    public IndexInput clone() {
        return new WrappingIndexInput(this);
    }

}
