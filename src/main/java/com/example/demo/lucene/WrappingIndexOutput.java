package com.example.demo.lucene;

import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.OutputStreamIndexOutput;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class WrappingIndexOutput extends IndexOutput {

    private static int DELEGATE_BUFFER_SIZE = 16384;

    private IndexOutput original;

    private ByteArrayOutputStream outputStream;

    private OutputStreamIndexOutput delegate;

    private WrappingEncoder encoder;

    private final int bufferSize;

    protected WrappingIndexOutput(IndexOutput original, WrappingEncoder encoder, int bufferSize) throws IOException {
        super(original.toString(), original.getName());

        // Copy original
        this.original = original;

        // Copy buffer size
        this.bufferSize = bufferSize;

        // Copy encoder
        this.encoder = encoder;

        // Create in memory output stream
        this.outputStream = new ByteArrayOutputStream();

        // Create IndexOutput delegate
        this.delegate = new OutputStreamIndexOutput(
                "OutputStreamIndexOutput(" + original.toString() + ")",
                original.getName(),
                this.outputStream,
                DELEGATE_BUFFER_SIZE
        );
    }

    @Override
    public void close() throws IOException {

        // Close delegate
        this.delegate.close();

        // Get bytes from OutputStream
        byte [] outputData = outputStream.toByteArray();

        // If something was ever written into the file
        if (outputData.length > 0) {

            // Copy length
            int length = outputData.length;

            // Calculate number of chunks
            int chunks = (int) Math.ceil( ((double) length) / ((double) bufferSize) );

            // Write length of encoded data
            original.writeLong(length);

            // For every chunk
            for (int i = 0; i < chunks; i++) {

                // Calculate chunk length
                int chunkLength = (int) Math.min(bufferSize, length);

                // Create byte array with length of chunk
                byte[] chunk = new byte[chunkLength];

                // Copy chunk segment into byte array
                System.arraycopy(outputData, i * bufferSize, chunk, 0, chunkLength);

                // Encode chunk
                byte[] encodedChunk = encoder.encode(chunk);

                // Write chunk encoded length
                this.original.writeInt(encodedChunk.length);

                // Write chunk decoded length
                this.original.writeInt(chunk.length);

                // Write chunk to original
                original.writeBytes(encodedChunk, 0, encodedChunk.length);

                // Decrement length by chunk length
                length -= chunkLength;
            }

        }

        // Close original
        this.original.close();
    }

    @Override
    public long getFilePointer() {
        return this.delegate.getFilePointer();
    }

    @Override
    public long getChecksum() throws IOException {
        return this.delegate.getChecksum();
    }

    @Override
    public void writeByte(byte b) throws IOException {
        this.delegate.writeByte(b);
    }

    @Override
    public void writeBytes(byte[] b, int offset, int length) throws IOException {
        this.delegate.writeBytes(b, offset, length);
    }

}
