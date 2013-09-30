/*
 * Copyright 2013 EMC Corporation. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://www.apache.org/licenses/LICENSE-2.0.txt
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.emc.esu.api.rest;

import java.io.IOException;
import java.io.InputStream;

import org.apache.http.HttpResponse;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;

public class CommonsInputStreamWrapper extends InputStream {
    private static final Logger l4j = Logger.getLogger( CommonsInputStreamWrapper.class );
    
    private InputStream in;
    private HttpResponse response;

    public CommonsInputStreamWrapper(HttpResponse response) throws IllegalStateException, IOException {
        this.in = response.getEntity().getContent();
        this.response = response;
    }

    @Override
    public int available() throws IOException {
        return in.available();
    }

    @Override
    public void close() throws IOException {
        in.close();
        if( response != null ) {
            EntityUtils.consume( response.getEntity() );
            response = null;
        }
    }

    @Override
    public synchronized void mark(int readlimit) {
        in.mark(readlimit);
    }

    @Override
    public boolean markSupported() {
        return in.markSupported();
    }

    @Override
    public int read() throws IOException {
        return in.read();
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        return in.read(b, off, len);
    }

    @Override
    public int read(byte[] b) throws IOException {
        return in.read(b);
    }

    @Override
    public synchronized void reset() throws IOException {
        in.reset();
    }

    @Override
    public long skip(long n) throws IOException {
        return in.skip(n);
    }

    @Override
    protected void finalize() throws Throwable {
        if( response != null ) {
            l4j.warn( "Warning: connection was not closed!" );
            try {
                EntityUtils.consume( response.getEntity() );
                response = null;
            } catch( Exception e ) {
                // Ignore
            }
        }
        super.finalize();
    }

    
}
