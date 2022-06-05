package com.example.demo.lucene;

import org.apache.lucene.store.*;

import java.io.IOException;
import java.util.Collection;
import java.util.Set;

public class WrappingDirectory extends Directory {

    private static final int BYTE_BUFFER_SIZE = 16384;

    private Directory delegate;

    private WrappingEncoder encoder;

    private WrappingDecoder decoder;

    public WrappingDirectory(Directory delegate, WrappingEncoder encoder, WrappingDecoder decoder) {
        this.delegate = delegate;
        this.encoder = encoder;
        this.decoder = decoder;
    }

    @Override
    public String[] listAll() throws IOException {
        return delegate.listAll();
    }

    @Override
    public void deleteFile(String name) throws IOException {
        delegate.deleteFile(name);
    }

    @Override
    public long fileLength(String name) throws IOException {
        return delegate.fileLength(name);
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) throws IOException {
        return new WrappingIndexOutput(delegate.createOutput(name, context), encoder, BYTE_BUFFER_SIZE);
    }

    @Override
    public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) throws IOException {
        return new WrappingIndexOutput(delegate.createTempOutput(prefix, suffix, context), encoder, BYTE_BUFFER_SIZE);
    }

    @Override
    public void sync(Collection<String> names) throws IOException {
        delegate.sync(names);
    }

    @Override
    public void syncMetaData() throws IOException {
        delegate.syncMetaData();
    }

    @Override
    public void rename(String source, String dest) throws IOException {
        delegate.rename(source, dest);
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        return new WrappingIndexInput(this.delegate, name, context, decoder, BYTE_BUFFER_SIZE);
    }

    @Override
    public Lock obtainLock(String name) throws IOException {
        return delegate.obtainLock(name);
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    @Override
    public Set<String> getPendingDeletions() throws IOException {
        return delegate.getPendingDeletions();
    }
}
