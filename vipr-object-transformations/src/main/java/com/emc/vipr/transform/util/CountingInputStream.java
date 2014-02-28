package com.emc.vipr.transform.util;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

public class CountingInputStream extends FilterInputStream {
    long count;

    public CountingInputStream(InputStream in) {
        super(in);
        count = 0;
    }

    @Override
    public int read() throws IOException {
        int c = in.read();
        if(c != -1) {
            count++;
        }
        
        return c;
    }
    
    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        int c = in.read(b, off, len);
        if(c != -1) {
            count += c;
        }
        
        return c;
    }
    
    @Override
    public int read(byte[] b) throws IOException {
        int c = in.read(b);
        
        if(c != -1) {
            count += c;
        }
        
        return c;
    }

    public long getByteCount() {
        return count;
    }
    
    
}
