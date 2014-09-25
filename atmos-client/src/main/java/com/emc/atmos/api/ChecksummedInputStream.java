package com.emc.atmos.api;

import com.emc.atmos.ChecksumError;

import java.io.IOException;
import java.io.InputStream;
import java.security.NoSuchAlgorithmException;

public class ChecksummedInputStream extends InputStream {
    private InputStream delegate;
    private ChecksumValue referenceChecksum;
    private RunningChecksum runningChecksum;

    public ChecksummedInputStream(InputStream delegate, ChecksumValue checksum) throws NoSuchAlgorithmException {
        this.delegate = delegate;
        this.referenceChecksum = checksum;
        this.runningChecksum = new RunningChecksum(checksum.getAlgorithm());
    }

    @Override
    public int read() throws IOException {
        int value = delegate.read();
        if (value < 0) finish();
        else update(new byte[]{(byte) value}, 0, 1);
        return value;
    }

    @Override
    public int read(byte[] b) throws IOException {
        return read(b, 0, b.length);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        int value = delegate.read(b, off, len);
        if (value < 0) finish();
        else update(b, off, value);
        return value;
    }

    @Override
    public long skip(long n) throws IOException {
        long remaining = n;
        while (remaining > 0) {
            int toSkip = (int) Math.min(remaining, (long) Integer.MAX_VALUE);
            int skipped = skip(toSkip);
            if (skipped == 0) break;
            remaining -= skipped;
        }
        return n - remaining;
    }

    private int skip(int n) throws IOException {
        byte[] bytes = new byte[1024 * 64]; // 32K
        int toRead, read, total = 0;
        while (total < n) {
            toRead = Math.min(n - total, bytes.length);
            read = delegate.read(bytes, 0, toRead);
            if (read < 0) {
                finish();
                break;
            }
            update(bytes, 0, read);
            total += read;
        }
        return total;
    }

    @Override
    public int available() throws IOException {
        return delegate.available();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    @Override
    public void mark(int readLimit) {
        throw new UnsupportedOperationException("mark not supported");
    }

    @Override
    public void reset() throws IOException {
        throw new UnsupportedOperationException("mark not supported");
    }

    @Override
    public boolean markSupported() {
        return false;
    }

    private void update(byte[] bytes, int offset, int length) {
        runningChecksum.update(bytes, offset, length);
    }

    private void finish() {
        String referenceValue = referenceChecksum.getValue();
        String calculatedValue = runningChecksum.getValue();
        if (!referenceValue.equals(calculatedValue))
            throw new ChecksumError("Checksum failure while reading stream", referenceValue, calculatedValue);
    }
}
