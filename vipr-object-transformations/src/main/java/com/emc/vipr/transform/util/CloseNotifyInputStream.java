package com.emc.vipr.transform.util;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

public class CloseNotifyInputStream extends FilterInputStream {
    private CloseCallback callback;

    public CloseNotifyInputStream(InputStream in, CloseCallback callback) {
        super(in);
        this.callback = callback;
    }
    
    @Override
    public void close() throws IOException {
        super.close();
        callback.closed(this);
    }
    
    

}
