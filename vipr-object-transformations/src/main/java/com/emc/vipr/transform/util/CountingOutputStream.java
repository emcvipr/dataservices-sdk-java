/**
 * 
 */
package com.emc.vipr.transform.util;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * @author cwikj
 *
 */
public class CountingOutputStream extends FilterOutputStream {
    private long byteCount;

    /**
     * @param out
     */
    public CountingOutputStream(OutputStream out) {
        super(out);
        byteCount = 0;
    }
    
    
    public long getByteCount() {
        return byteCount;
    }


    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        byteCount += len;
        out.write(b, off, len);
    }


    @Override
    public void write(byte[] b) throws IOException {
        byteCount += b.length;
        out.write(b);
    }


    @Override
    public void write(int b) throws IOException {
        byteCount++;
        out.write(b);
    }

}
