package com.emc.vipr.transform.util;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class CloseNotifyOutputStream extends FilterOutputStream {
    private CloseCallback callback;
    private boolean closed;

    public CloseNotifyOutputStream(OutputStream out, CloseCallback callback) {
        super(out);
        this.callback = callback;
        this.closed = false;
    }

    @Override
    public void close() throws IOException {
        out.close();
        closed = true;
        if(callback != null) {
            callback.closed(this);
        }
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        if(closed) {
            throw new IOException("Stream closed");
        }
        out.write(b, off, len);
    }

    @Override
    public void write(byte[] b) throws IOException {
        if(closed) {
            throw new IOException("Stream closed");
        }
        out.write(b);
    }

    @Override
    public void flush() throws IOException {
        if(closed) {
            throw new IOException("Stream closed");
        }
        out.flush();
    }

    @Override
    public void write(int b) throws IOException {
        if(closed) {
            throw new IOException("Stream closed");
        }
        out.write(b);
    }

    public boolean isClosed() {
        return closed;
    }
    
    
    
}
